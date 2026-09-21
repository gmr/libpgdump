//! Weighted topological sorting of TOC entries.
//!
//! Implements the same sorting strategy as pg_dump's `pg_dump_sort.c`:
//!
//! 1. **Initial sort** by object-type priority, then namespace, then tag name.
//!    This produces a deterministic ordering that is cosmetically similar to
//!    what `pg_dump` emits.
//!
//! 2. **Topological sort** using Kahn's algorithm with a max-heap priority
//!    queue (Knuth's approach).  This reorders entries only as needed to
//!    satisfy dependency constraints, preserving the initial ordering wherever
//!    possible.

use std::collections::{BinaryHeap, HashMap};

use crate::entry::Entry;
use crate::types::ObjectType;

/// Compare `Option<String>` with `Some` sorting before `None`.
///
/// This is the inverse of Rust's default `Option::cmp` (which puts `None`
/// first).  pg_dump sorts entries with a namespace/tag before those without,
/// so `Some` must compare as less than `None`.
fn cmp_opt_str(a: &Option<String>, b: &Option<String>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.cmp(b),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

/// The phase-1 sort key for one entry, mirroring `DOTypeNameCompare` in
/// pg_dump_sort.c.
///
/// A COMMENT, SECURITY LABEL or ACL does not carry a key of its own:
/// pg_dump writes these while it writes the object they attach to, so they
/// land directly after that object rather than at the priority their own
/// type would give them.  Such an entry borrows the key of the entry it
/// depends on and sorts just behind it, which no fixed value in
/// `ObjectType::priority()` could express.
#[derive(Clone)]
struct SortKey {
    priority: i32,
    namespace: Option<String>,
    /// The name pg_dump sorts on, which is not always the whole tag.
    tag: Option<String>,
    desc: ObjectType,
    /// 0 for an entry sorting on its own key, otherwise its rank behind the
    /// target it attaches to.
    trailing: u8,
    /// How many attachment steps separate this entry from the target whose
    /// key it borrows.  A column ACL trails its table-level ACL, so it is
    /// two steps behind the table and sorts after the one-step ACL.
    depth: u8,
    /// Full tag, to keep entries that share a sort name in a stable order.
    own_tag: Option<String>,
}

/// Where an attachment sorts behind the object it describes, or `None` for
/// an object that sorts on its own key.
///
/// pg_dump emits an object, then its comment, then its security label, then
/// its ACL.
fn trailing_rank(desc: &ObjectType) -> Option<u8> {
    match desc {
        ObjectType::Comment => Some(1),
        ObjectType::SecurityLabel => Some(2),
        ObjectType::Acl => Some(3),
        _ => None,
    }
}

/// Object types whose TOC tag is `"<table> <name>"`.
///
/// pg_dump builds these tags for display, but sorts the objects on their own
/// name — the second part — so a constraint called `a_nn` on table `nn`
/// sorts ahead of `bookings_pk` on table `bookings`.
fn tag_is_table_qualified(desc: &ObjectType) -> bool {
    matches!(
        desc,
        ObjectType::CheckConstraint
            | ObjectType::Constraint
            | ObjectType::Default
            | ObjectType::FkConstraint
            | ObjectType::Policy
            | ObjectType::Rule
            | ObjectType::Trigger
    )
}

/// The tag of the table that qualifies `entry`, taken from its dependencies.
///
/// pg_dump writes a table-qualified tag as `"<table> <name>"` without quoting
/// the table name, so a table called `order items` gives `order items a_pk`
/// and the tag alone cannot say where the table name ends.  The owning table
/// is a dependency of the entry, so its tag supplies the prefix.  An FK
/// constraint depends on two tables, and the longest matching prefix is the
/// one that qualifies the tag.
fn owner_tag<'a>(
    entry: &Entry,
    entries: &'a [Entry],
    id_to_idx: &HashMap<i32, usize>,
) -> Option<&'a str> {
    let tag = entry.tag.as_deref()?;
    entry
        .dependencies
        .iter()
        .filter_map(|id| id_to_idx.get(id))
        .filter_map(|&idx| entries[idx].tag.as_deref())
        .filter(|owner| {
            tag.len() > owner.len()
                && tag.as_bytes()[owner.len()] == b' '
                && tag.starts_with(*owner)
        })
        .max_by_key(|owner| owner.len())
}

/// The part of a table-qualified tag that pg_dump sorts on, for an entry
/// whose owning table is not in the archive.
///
/// This splits at the first space, which is correct unless the table name
/// itself contains one.  A tag that does not have the expected shape is used
/// whole.
fn strip_table_qualifier(tag: &str) -> &str {
    match tag.split_once(' ') {
        Some((_, name)) => name,
        None => tag,
    }
}

/// The name pg_dump sorts `entry` by.
fn sort_name(entry: &Entry, owner: Option<&str>) -> Option<String> {
    match &entry.tag {
        Some(tag) if tag_is_table_qualified(&entry.desc) => Some(match owner {
            Some(owner) => tag[owner.len() + 1..].to_string(),
            None => strip_table_qualifier(tag).to_string(),
        }),
        other => other.clone(),
    }
}

impl SortKey {
    fn own(entry: &Entry, owner: Option<&str>) -> Self {
        SortKey {
            priority: entry.desc.priority(),
            namespace: entry.namespace.clone(),
            tag: sort_name(entry, owner),
            desc: entry.desc.clone(),
            trailing: 0,
            depth: 0,
            own_tag: entry.tag.clone(),
        }
    }

    /// The key that sorts `entry` directly after the entry keyed by `target`.
    fn trailing(entry: &Entry, target: &SortKey, rank: u8) -> Self {
        SortKey {
            trailing: rank,
            depth: target.depth + 1,
            own_tag: entry.tag.clone(),
            ..target.clone()
        }
    }

    /// Whether both keys borrow from, or belong to, the same target entry.
    fn same_target(&self, other: &Self) -> bool {
        self.priority == other.priority
            && self.namespace == other.namespace
            && self.tag == other.tag
            && self.desc == other.desc
    }

    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| cmp_opt_str(&self.namespace, &other.namespace))
            .then_with(|| cmp_opt_str(&self.tag, &other.tag))
            .then_with(|| self.desc.cmp(&other.desc))
            .then_with(|| self.trailing.cmp(&other.trailing))
            .then_with(|| self.depth.cmp(&other.depth))
            .then_with(|| cmp_opt_str(&self.own_tag, &other.own_tag))
    }
}

/// How far an attachment chain is followed: an object, its ACL, and the
/// column ACL that trails that ACL.
const MAX_ATTACHMENT_DEPTH: u8 = 3;

/// Build the phase-1 sort key for one entry.
///
/// An attachment trails the entry it depends on.  Every dependency it has
/// must lead to the same target, so an attachment with unrelated
/// dependencies keeps a key of its own.  A column ACL depends on both its
/// table and the table-level ACL; both lead to the table, and the chain
/// through the ACL is the longer one, so the column ACL sorts behind it.
fn sort_key(entries: &[Entry], id_to_idx: &HashMap<i32, usize>, idx: usize, budget: u8) -> SortKey {
    let entry = &entries[idx];
    let own = || SortKey::own(entry, owner_tag(entry, entries, id_to_idx));

    let Some(rank) = trailing_rank(&entry.desc) else {
        return own();
    };
    if budget == 0 || entry.dependencies.is_empty() {
        return own();
    }

    let mut target: Option<SortKey> = None;
    for dep_id in &entry.dependencies {
        let Some(&dep) = id_to_idx.get(dep_id) else {
            return own();
        };
        if dep == idx {
            return own();
        }
        let key = sort_key(entries, id_to_idx, dep, budget - 1);
        target = match target {
            None => Some(key),
            Some(prev) if prev.same_target(&key) => {
                Some(if key.depth > prev.depth { key } else { prev })
            }
            Some(_) => return own(),
        };
    }

    match target {
        Some(target) => SortKey::trailing(entry, &target, rank),
        None => own(),
    }
}

/// Build the phase-1 sort key for every entry.
fn sort_keys(entries: &[Entry]) -> Vec<SortKey> {
    let id_to_idx = build_id_to_idx(entries);
    (0..entries.len())
        .map(|idx| sort_key(entries, &id_to_idx, idx, MAX_ATTACHMENT_DEPTH))
        .collect()
}

/// Sort entries using the same two-phase strategy as pg_dump:
///
/// 1. Stable sort by type-priority / namespace / name.
/// 2. Topological sort respecting dependencies, using a binary heap to
///    preserve the phase-1 ordering wherever dependencies allow.
///
/// If the dependency graph contains a cycle, the sort cannot place every
/// entry.  As in pg_dump's `findDependencyLoops` / `repairDependencyLoop`,
/// one edge of the cycle is dropped and the sort runs again, until it
/// succeeds.  Only the sort's working copy of the dependency lists is
/// modified; `Entry::dependencies` is left intact.
pub(crate) fn sort_entries(entries: &mut Vec<Entry>) {
    if entries.len() <= 1 {
        return;
    }

    // Phase 1: cosmetic type/name sort
    let keys = sort_keys(entries);
    let mut ordering: Vec<usize> = (0..entries.len()).collect();
    ordering.sort_by(|&a, &b| keys[a].cmp(&keys[b]));
    apply_ordering(entries, ordering);

    // Phase 2: topological sort with heap-based tie-breaking
    topo_sort(entries);
}

/// Map `dump_id` → index in `entries`.
///
/// Sparse, because `dump_id` values come from the archive and need not be
/// dense or small.
fn build_id_to_idx(entries: &[Entry]) -> HashMap<i32, usize> {
    let mut id_to_idx = HashMap::with_capacity(entries.len());
    for (i, e) in entries.iter().enumerate() {
        if e.dump_id > 0 {
            id_to_idx.insert(e.dump_id, i);
        }
    }
    id_to_idx
}

/// Topologically sort `entries`, repairing dependency cycles as needed.
fn topo_sort(entries: &mut Vec<Entry>) {
    let id_to_idx = build_id_to_idx(entries);

    // Working copy of the dependency graph, as indices.  Repairs drop edges
    // from this copy, never from the entries themselves.
    let mut deps: Vec<Vec<usize>> = entries
        .iter()
        .map(|e| {
            e.dependencies
                .iter()
                .filter_map(|id| id_to_idx.get(id).copied())
                .collect()
        })
        .collect();

    let ordering = loop {
        match try_topo_sort(&deps) {
            Ok(ordering) => break ordering,
            Err(unplaced) => match find_cycle_edge(&unplaced.entries, &deps) {
                // Drop one edge of the cycle and try again.  Each repair
                // removes an edge, so this terminates.
                Some((from, pos)) => {
                    deps[from].remove(pos);
                }
                // Unreachable in principle — entries left over by Kahn's
                // algorithm always contain a cycle — but breaking out beats
                // looping forever if that ever stops holding.
                None => break unplaced.finish_appending(),
            },
        }
    };

    apply_ordering(entries, ordering);
}

/// Permute `entries` into `ordering`, moving rather than cloning.
fn apply_ordering(entries: &mut Vec<Entry>, ordering: Vec<usize>) {
    let mut old_entries: Vec<Option<Entry>> =
        std::mem::take(entries).into_iter().map(Some).collect();
    entries.extend(ordering.into_iter().map(|i| old_entries[i].take().unwrap()));
}

/// Kahn's algorithm with a max-heap, matching pg_dump's `TopoSort`.
///
/// The heap ensures that, among all entries whose dependencies are satisfied,
/// the one with the highest index in the *current* (phase-1-sorted) array is
/// emitted first — which, when filling the output array backwards, preserves
/// the cosmetic ordering as much as possible.
///
/// Returns the ordering on success, or the partial result on failure.
fn try_topo_sort(deps: &[Vec<usize>]) -> Result<Vec<usize>, PartialOrdering> {
    let n = deps.len();

    // For each entry, count how many other entries list it as a dependency
    // (i.e. how many entries must come *after* it).  pg_dump calls this
    // `beforeConstraints` — the number of constraints saying "this item
    // must be before something else".
    //
    // In pg_dump's model: entry A depends on entry B means B must come
    // before A.  So for each dep B in A.dependencies, B gets a +1 in
    // before_constraints, because B is constrained to appear before A.
    let mut before_constraints: Vec<usize> = vec![0; n];
    for entry_deps in deps {
        for &dep in entry_deps {
            before_constraints[dep] += 1;
        }
    }

    let mut heap: BinaryHeap<usize> = (0..n).filter(|&i| before_constraints[i] == 0).collect();

    // Fill output backwards (highest-index first from the heap)
    let mut ordering: Vec<usize> = vec![0; n];
    let mut out_pos = n;
    while let Some(idx) = heap.pop() {
        out_pos -= 1;
        ordering[out_pos] = idx;
        for &dep in &deps[idx] {
            before_constraints[dep] -= 1;
            if before_constraints[dep] == 0 {
                heap.push(dep);
            }
        }
    }

    if out_pos == 0 {
        return Ok(ordering);
    }

    // Anything still carrying a before-constraint was never emitted.
    Err(PartialOrdering {
        placed: ordering.split_off(out_pos),
        entries: (0..n).filter(|&i| before_constraints[i] > 0).collect(),
    })
}

/// The result of a topological sort that hit a dependency cycle: the entries
/// it did place, in order, and the ones it could not.
struct PartialOrdering {
    placed: Vec<usize>,
    entries: Vec<usize>,
}

impl PartialOrdering {
    /// Give up on the unplaced entries and put them at the end, where they
    /// do the least damage: everything else keeps its section and priority
    /// ordering, which is what decides whether the archive restores.
    fn finish_appending(mut self) -> Vec<usize> {
        self.placed.extend(self.entries);
        self.placed
    }
}

/// Find one back edge of a cycle among `unplaced`, as `(entry, position in
/// its dependency list)`.
///
/// The unplaced set is closed under dependency edges — if an entry could not
/// be placed, neither could anything it depends on — so a depth-first search
/// starting inside it stays inside it.
fn find_cycle_edge(unplaced: &[usize], deps: &[Vec<usize>]) -> Option<(usize, usize)> {
    const WHITE: u8 = 0;
    const GRAY: u8 = 1;
    const BLACK: u8 = 2;

    let mut color = vec![WHITE; deps.len()];
    for &start in unplaced {
        if color[start] != WHITE {
            continue;
        }
        color[start] = GRAY;
        let mut stack: Vec<(usize, usize)> = vec![(start, 0)];
        while let Some(&mut (node, ref mut next)) = stack.last_mut() {
            let Some(&child) = deps[node].get(*next) else {
                color[node] = BLACK;
                stack.pop();
                continue;
            };
            let pos = *next;
            *next += 1;
            match color[child] {
                GRAY => return Some((node, pos)),
                WHITE => {
                    color[child] = GRAY;
                    stack.push((child, 0));
                }
                _ => {}
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ObjectType, OffsetState};

    fn make_entry(
        dump_id: i32,
        desc: ObjectType,
        namespace: Option<&str>,
        tag: Option<&str>,
        deps: Vec<i32>,
    ) -> Entry {
        Entry {
            dump_id,
            had_dumper: false,
            table_oid: "0".to_string(),
            oid: "0".to_string(),
            tag: tag.map(String::from),
            desc: desc.clone(),
            section: desc.section(),
            defn: None,
            drop_stmt: None,
            copy_stmt: None,
            namespace: namespace.map(String::from),
            tablespace: None,
            tableam: None,
            relkind: None,
            owner: None,
            with_oids: false,
            dependencies: deps,
            data_state: OffsetState::NoData,
            offset: 0,
            filename: None,
        }
    }

    #[test]
    fn test_type_priority_ordering() {
        assert!(ObjectType::Schema.priority() < ObjectType::Table.priority());
        assert!(ObjectType::Table.priority() < ObjectType::TableData.priority());
        assert!(ObjectType::TableData.priority() < ObjectType::Index.priority());
        assert!(ObjectType::Index.priority() < ObjectType::FkConstraint.priority());
        assert!(ObjectType::FkConstraint.priority() < ObjectType::EventTrigger.priority());
    }

    #[test]
    fn test_sort_by_type_priority() {
        let mut entries = vec![
            make_entry(
                1,
                ObjectType::Index,
                Some("public"),
                Some("idx_test"),
                vec![],
            ),
            make_entry(2, ObjectType::Table, Some("public"), Some("test"), vec![]),
            make_entry(3, ObjectType::Schema, None, Some("public"), vec![]),
        ];
        sort_entries(&mut entries);
        assert_eq!(entries[0].desc, ObjectType::Schema);
        assert_eq!(entries[1].desc, ObjectType::Table);
        assert_eq!(entries[2].desc, ObjectType::Index);
    }

    #[test]
    fn test_sort_respects_dependencies() {
        // Table depends on schema, index depends on table
        let mut entries = vec![
            make_entry(3, ObjectType::Index, Some("public"), Some("idx_a"), vec![2]),
            make_entry(2, ObjectType::Table, Some("public"), Some("a"), vec![1]),
            make_entry(1, ObjectType::Schema, None, Some("public"), vec![]),
        ];
        sort_entries(&mut entries);
        // Schema before table, table before index
        let ids: Vec<i32> = entries.iter().map(|e| e.dump_id).collect();
        let schema_pos = ids.iter().position(|&id| id == 1).unwrap();
        let table_pos = ids.iter().position(|&id| id == 2).unwrap();
        let index_pos = ids.iter().position(|&id| id == 3).unwrap();
        assert!(schema_pos < table_pos);
        assert!(table_pos < index_pos);
    }

    #[test]
    fn test_sort_namespace_ordering() {
        let mut entries = vec![
            make_entry(2, ObjectType::Table, Some("public"), Some("b"), vec![]),
            make_entry(1, ObjectType::Table, Some("app"), Some("a"), vec![]),
        ];
        sort_entries(&mut entries);
        // "app" namespace sorts before "public"
        assert_eq!(entries[0].namespace.as_deref(), Some("app"));
        assert_eq!(entries[1].namespace.as_deref(), Some("public"));
    }

    #[test]
    fn test_sort_name_ordering_within_type() {
        let mut entries = vec![
            make_entry(2, ObjectType::Table, Some("public"), Some("zebra"), vec![]),
            make_entry(1, ObjectType::Table, Some("public"), Some("alpha"), vec![]),
        ];
        sort_entries(&mut entries);
        assert_eq!(entries[0].tag.as_deref(), Some("alpha"));
        assert_eq!(entries[1].tag.as_deref(), Some("zebra"));
    }

    #[test]
    fn test_sort_namespace_then_tag_across_equal_priority_types() {
        // Table and ForeignTable share the same priority (22).
        // When priority is equal, entries should still sort by
        // namespace then tag, regardless of ObjectType variant.
        let mut entries = vec![
            make_entry(
                1,
                ObjectType::ForeignTable,
                Some("public"),
                Some("beta"),
                vec![],
            ),
            make_entry(2, ObjectType::Table, Some("app"), Some("alpha"), vec![]),
            make_entry(3, ObjectType::Table, Some("public"), Some("alpha"), vec![]),
            make_entry(
                4,
                ObjectType::ForeignTable,
                Some("app"),
                Some("zeta"),
                vec![],
            ),
        ];

        sort_entries(&mut entries);

        // All four share priority 22, so ordering is by namespace
        // then tag: app.alpha, app.zeta, public.alpha, public.beta
        assert_eq!(entries[0].namespace.as_deref(), Some("app"));
        assert_eq!(entries[0].tag.as_deref(), Some("alpha"));
        assert_eq!(entries[1].namespace.as_deref(), Some("app"));
        assert_eq!(entries[1].tag.as_deref(), Some("zeta"));
        assert_eq!(entries[2].namespace.as_deref(), Some("public"));
        assert_eq!(entries[2].tag.as_deref(), Some("alpha"));
        assert_eq!(entries[3].namespace.as_deref(), Some("public"));
        assert_eq!(entries[3].tag.as_deref(), Some("beta"));
    }

    #[test]
    fn test_sort_handles_empty() {
        let mut entries: Vec<Entry> = vec![];
        sort_entries(&mut entries);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_sort_single_entry() {
        let mut entries = vec![make_entry(
            1,
            ObjectType::Table,
            Some("public"),
            Some("t"),
            vec![],
        )];
        sort_entries(&mut entries);
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_strip_table_qualifier() {
        assert_eq!(strip_table_qualifier("nn a_nn"), "a_nn");
        // A tag that is not table-qualified is used whole.
        assert_eq!(strip_table_qualifier("a_nn"), "a_nn");
    }

    #[test]
    fn test_constraint_on_table_whose_name_has_a_space() {
        // pg_dump writes the table name into the tag unquoted, so only the
        // owning table's own tag says where that name ends.  Sorted by
        // constraint name, a_pk precedes bookings_pkey.
        let mut entries = vec![
            make_entry(1, ObjectType::Table, Some("t"), Some("order items"), vec![]),
            make_entry(2, ObjectType::Table, Some("t"), Some("bookings"), vec![]),
            make_entry(
                3,
                ObjectType::Constraint,
                Some("t"),
                Some("bookings bookings_pkey"),
                vec![2],
            ),
            make_entry(
                4,
                ObjectType::Constraint,
                Some("t"),
                Some("order items a_pk"),
                vec![1],
            ),
        ];

        sort_entries(&mut entries);

        let constraints: Vec<&str> = entries
            .iter()
            .filter(|e| e.desc == ObjectType::Constraint)
            .filter_map(|e| e.tag.as_deref())
            .collect();
        assert_eq!(constraints, ["order items a_pk", "bookings bookings_pkey"]);
    }

    #[test]
    fn test_column_acl_follows_the_table_acl() {
        // A column ACL depends on both the table and the table-level ACL,
        // and pg_dump writes it directly after that ACL.
        let mut entries = vec![
            make_entry(1, ObjectType::Table, Some("t"), Some("acls"), vec![]),
            make_entry(
                3,
                ObjectType::Acl,
                Some("t"),
                Some("COLUMN acls.open"),
                vec![1, 2],
            ),
            make_entry(2, ObjectType::Acl, Some("t"), Some("TABLE acls"), vec![1]),
        ];

        sort_entries(&mut entries);

        let order: Vec<&str> = entries.iter().filter_map(|e| e.tag.as_deref()).collect();
        assert_eq!(order, ["acls", "TABLE acls", "COLUMN acls.open"]);
    }

    #[test]
    fn test_constraints_sort_by_constraint_name() {
        // pg_dump sorts constraints on the constraint name, not on the
        // "<table> <constraint>" tag the archive stores.
        let mut entries = vec![
            make_entry(
                1,
                ObjectType::Constraint,
                Some("app"),
                Some("bookings bookings_pk"),
                vec![],
            ),
            make_entry(
                2,
                ObjectType::Constraint,
                Some("app"),
                Some("nn a_nn"),
                vec![],
            ),
        ];
        sort_entries(&mut entries);
        assert_eq!(entries[0].tag.as_deref(), Some("nn a_nn"));
        assert_eq!(entries[1].tag.as_deref(), Some("bookings bookings_pk"));
    }

    #[test]
    fn test_attachments_follow_their_target() {
        // COMMENT, SECURITY LABEL and ACL sort directly behind the object
        // they describe, in that order.
        let mut entries = vec![
            make_entry(1, ObjectType::Schema, None, Some("app"), vec![]),
            make_entry(2, ObjectType::Acl, Some(""), Some("SCHEMA app"), vec![1]),
            make_entry(3, ObjectType::Table, Some("app"), Some("t"), vec![1]),
            make_entry(
                4,
                ObjectType::Comment,
                Some(""),
                Some("SCHEMA app"),
                vec![1],
            ),
            make_entry(
                5,
                ObjectType::SecurityLabel,
                Some(""),
                Some("SCHEMA app"),
                vec![1],
            ),
        ];
        sort_entries(&mut entries);
        let ids: Vec<i32> = entries.iter().map(|e| e.dump_id).collect();
        assert_eq!(ids, vec![1, 4, 5, 2, 3]);
    }

    #[test]
    fn test_attachment_without_a_target_keeps_its_own_priority() {
        // The commented object is not in this archive, so the comment falls
        // back to sorting on its own type.
        let mut entries = vec![
            make_entry(1, ObjectType::Comment, Some(""), Some("SCHEMA x"), vec![99]),
            make_entry(2, ObjectType::Schema, None, Some("app"), vec![]),
        ];
        sort_entries(&mut entries);
        let ids: Vec<i32> = entries.iter().map(|e| e.dump_id).collect();
        assert_eq!(ids, vec![2, 1]);
    }

    #[test]
    fn test_dependency_cycle_handled() {
        // Circular dependency — sort should not panic
        let mut entries = vec![
            make_entry(1, ObjectType::Table, Some("public"), Some("a"), vec![2]),
            make_entry(2, ObjectType::Table, Some("public"), Some("b"), vec![1]),
        ];
        sort_entries(&mut entries);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_cycle_members_sort_after_their_schema() {
        // Two tables that depend on each other, both in a schema they also
        // depend on.  The cycle between them cannot be satisfied, but the
        // schema must still come first.
        let mut entries = vec![
            make_entry(1, ObjectType::Schema, None, Some("app"), vec![]),
            make_entry(2, ObjectType::Table, Some("app"), Some("a"), vec![1, 3]),
            make_entry(3, ObjectType::Table, Some("app"), Some("b"), vec![1, 2]),
        ];
        sort_entries(&mut entries);

        let ids: Vec<i32> = entries.iter().map(|e| e.dump_id).collect();
        assert_eq!(ids[0], 1, "schema must precede the cycle members: {ids:?}");
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn test_cycle_repair_keeps_dependencies_intact() {
        // The repair pass drops edges from its own working copy only.
        let mut entries = vec![
            make_entry(1, ObjectType::Table, Some("public"), Some("a"), vec![2]),
            make_entry(2, ObjectType::Table, Some("public"), Some("b"), vec![1]),
        ];
        sort_entries(&mut entries);
        for e in &entries {
            assert_eq!(e.dependencies.len(), 1);
        }
    }

    #[test]
    fn test_cycle_does_not_disturb_unrelated_entries() {
        // A cycle between two tables must not move an index that depends on
        // one of them, nor the schema they all sit in.
        let mut entries = vec![
            make_entry(1, ObjectType::Schema, None, Some("app"), vec![]),
            make_entry(2, ObjectType::Table, Some("app"), Some("a"), vec![1, 3]),
            make_entry(3, ObjectType::Table, Some("app"), Some("b"), vec![1, 2]),
            make_entry(4, ObjectType::Index, Some("app"), Some("idx_a"), vec![2]),
        ];
        sort_entries(&mut entries);

        let ids: Vec<i32> = entries.iter().map(|e| e.dump_id).collect();
        let pos = |id: i32| ids.iter().position(|&i| i == id).unwrap();
        assert!(pos(1) < pos(2), "{ids:?}");
        assert!(pos(1) < pos(3), "{ids:?}");
        assert!(pos(2) < pos(4), "{ids:?}");
    }

    #[test]
    fn test_self_dependency_is_repaired() {
        let mut entries = vec![
            make_entry(1, ObjectType::Schema, None, Some("app"), vec![]),
            make_entry(2, ObjectType::Table, Some("app"), Some("a"), vec![1, 2]),
        ];
        sort_entries(&mut entries);
        let ids: Vec<i32> = entries.iter().map(|e| e.dump_id).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn test_entries_without_dump_ids_are_placed() {
        let mut entries = vec![
            make_entry(0, ObjectType::Encoding, None, None, vec![]),
            make_entry(1, ObjectType::Table, Some("app"), Some("a"), vec![]),
        ];
        sort_entries(&mut entries);
        assert_eq!(entries[0].desc, ObjectType::Encoding);
        assert_eq!(entries[1].desc, ObjectType::Table);
    }
}
