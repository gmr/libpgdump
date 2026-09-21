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

use std::collections::BinaryHeap;

use crate::entry::Entry;

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

/// Compare two entries by type priority, then namespace, then tag.
///
/// This mirrors `DOTypeNameCompare` in pg_dump_sort.c.
fn entry_cmp(a: &Entry, b: &Entry) -> std::cmp::Ordering {
    a.desc
        .priority()
        .cmp(&b.desc.priority())
        .then_with(|| cmp_opt_str(&a.namespace, &b.namespace))
        .then_with(|| cmp_opt_str(&a.tag, &b.tag))
        .then_with(|| a.desc.cmp(&b.desc))
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
    entries.sort_by(entry_cmp);

    // Phase 2: topological sort with heap-based tie-breaking
    topo_sort(entries);
}

/// Map `dump_id` → index in `entries`, as a lookup table indexed by dump_id.
fn build_id_to_idx(entries: &[Entry]) -> Vec<Option<usize>> {
    let max_id = entries.iter().map(|e| e.dump_id).max().unwrap_or(0);
    let mut id_to_idx: Vec<Option<usize>> = vec![None; (max_id.max(0) + 1) as usize];
    for (i, e) in entries.iter().enumerate() {
        if e.dump_id > 0 {
            id_to_idx[e.dump_id as usize] = Some(i);
        }
    }
    id_to_idx
}

/// Resolve a dependency id to an index, ignoring ids not in the entry set.
fn dep_idx(id_to_idx: &[Option<usize>], dep_id: i32) -> Option<usize> {
    if dep_id > 0 && (dep_id as usize) < id_to_idx.len() {
        id_to_idx[dep_id as usize]
    } else {
        None
    }
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
                .filter_map(|&id| dep_idx(&id_to_idx, id))
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

    // Reorder entries according to `ordering` using moves (no cloning)
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
