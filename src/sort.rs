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
        .then_with(|| a.desc.to_string().cmp(&b.desc.to_string()))
}

/// Sort entries using the same two-phase strategy as pg_dump:
///
/// 1. Stable sort by type-priority / namespace / name.
/// 2. Topological sort respecting dependencies, using a binary heap to
///    preserve the phase-1 ordering wherever dependencies allow.
pub(crate) fn sort_entries(entries: &mut Vec<Entry>) {
    if entries.len() <= 1 {
        return;
    }

    // Phase 1: cosmetic type/name sort
    entries.sort_by(entry_cmp);

    // Phase 2: topological sort with heap-based tie-breaking
    topo_sort(entries);
}

/// Kahn's algorithm with a max-heap, matching pg_dump's `TopoSort`.
///
/// The heap ensures that, among all entries whose dependencies are satisfied,
/// the one with the highest index in the *current* (phase-1-sorted) array is
/// emitted first — which, when filling the output array backwards, preserves
/// the cosmetic ordering as much as possible.
fn topo_sort(entries: &mut Vec<Entry>) {
    let n = entries.len();

    // Build a map from dump_id → index in `entries`.
    let max_id = entries.iter().map(|e| e.dump_id).max().unwrap_or(0);
    if max_id <= 0 {
        return;
    }
    let mut id_to_idx: Vec<Option<usize>> = vec![None; (max_id + 1) as usize];
    for (i, e) in entries.iter().enumerate() {
        if e.dump_id > 0 {
            id_to_idx[e.dump_id as usize] = Some(i);
        }
    }

    // For each entry, count how many other entries list it as a dependency
    // (i.e. how many entries must come *after* it).  pg_dump calls this
    // `beforeConstraints` — the number of constraints saying "this item
    // must be before something else".  But it's computed by iterating each
    // object's dependency list and incrementing the count for each dep.
    //
    // In pg_dump's model: entry A depends on entry B means B must come
    // before A.  So for each dep B in A.dependencies, B gets a +1 in
    // before_constraints, because B is constrained to appear before A.
    let mut before_constraints: Vec<i32> = vec![0; (max_id + 1) as usize];
    for e in entries.iter() {
        for &dep_id in &e.dependencies {
            if dep_id > 0 && (dep_id as usize) < before_constraints.len() {
                // Only count if the dependency actually exists in our entry set
                if id_to_idx[dep_id as usize].is_some() {
                    before_constraints[dep_id as usize] += 1;
                }
            }
        }
    }

    let mut heap = BinaryHeap::new();
    for (i, e) in entries.iter().enumerate() {
        if e.dump_id > 0 && before_constraints[e.dump_id as usize] == 0 {
            heap.push(i);
        }
    }

    // Fill output backwards (highest-index first from the heap)
    let mut ordering: Vec<usize> = vec![0; n];
    let mut out_pos = n;
    while let Some(idx) = heap.pop() {
        out_pos -= 1;
        ordering[out_pos] = idx;
        // Decrease before_constraints for each dependency of this entry
        let entry = &entries[idx];
        for &dep_id in &entry.dependencies {
            if dep_id > 0
                && (dep_id as usize) < before_constraints.len()
                && let Some(dep_idx) = id_to_idx[dep_id as usize]
            {
                before_constraints[dep_id as usize] -= 1;
                if before_constraints[dep_id as usize] == 0 {
                    heap.push(dep_idx);
                }
            }
        }
    }

    // If there are dependency cycles, remaining entries weren't emitted.
    // Append them in their original order (best-effort).
    if out_pos > 0 {
        let mut emitted = vec![false; n];
        for i in out_pos..n {
            emitted[ordering[i]] = true;
        }
        for (i, is_emitted) in emitted.iter().enumerate() {
            if !is_emitted {
                out_pos -= 1;
                ordering[out_pos] = i;
            }
        }
    }

    // Reorder entries according to `ordering` using moves (no cloning)
    let mut old_entries: Vec<Option<Entry>> =
        std::mem::take(entries).into_iter().map(Some).collect();
    entries.extend(ordering.into_iter().map(|i| old_entries[i].take().unwrap()));
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
}
