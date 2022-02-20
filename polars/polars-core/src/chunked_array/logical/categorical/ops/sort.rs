use crate::prelude::sort::{argsort_branch, order_default_null, order_reverse_null};
use crate::utils::NoNull;
use super::*;

impl CategoricalChunked {
    fn sort_with(&self, options: SortOptions) -> CategoricalChunked {
        assert!(
            !options.nulls_last,
            "null last not yet supported for categorical dtype"
        );

        if self.use_lexical_sort() {
            match &**self.get_rev_map() {
                RevMapping::Local(arr) => {
                    // we don't use arrow2 sort here because its not activated
                    // that saves compilation
                    let ca= Utf8Chunked::from_chunks("", vec![Arc::from(arr.clone())]);
                    let sorted = ca.sort(reverse);
                    let arr = sorted.downcast_iter().next().unwrap().clone();
                    let rev_map = RevMapping::Local(arr);
                    CategoricalChunked::from_cats_and_rev_map(self.logical().clone(), Arc::new(rev_map))
                },
                RevMapping::Global(_, _, _)  => {
                    // a global rev map must always point to the same string values
                    // so we cannot sort the categories.

                    let mut vals = self
                        .into_iter()
                        .zip(self.iter_str())
                        .collect_trusted::<Vec<_>>();

                    argsort_branch(
                        vals.as_mut_slice(),
                        options.descending,
                        |(_, a), (_, b)| order_default_null(a, b),
                        |(_, a), (_, b)| order_reverse_null(a, b),
                    );
                    let cats: NoNull<UInt32Chunked> = vals.into_iter().map(|(idx, _v)| idx).collect_trusted();
                    CategoricalChunked::from_cats_and_rev_map(cats.into_inner(), self.get_rev_map().clone())
                }
            }
        } else {
            let cats = self.logical.sort_with(options);
            CategoricalChunked::from_cats_and_rev_map(cats, self.get_rev_map().clone())
        }
    }

    /// Returned a sorted `ChunkedArray`.
    fn sort(&self, reverse: bool) -> CategoricalChunked {
        self.sort_with(SortOptions {
            nulls_last: false,
            descending: reverse,
        })
    }

    /// Retrieve the indexes needed to sort this array.
    fn argsort(&self, reverse: bool) -> IdxCa {
        if self.use_lexical_sort() {
            let mut count: IdxSize = 0;
            // safety: we know the iterators len
            let mut vals = self
                .iter_str()
                .map(|s| {
                    let i = count;
                    count += 1;
                    (i, s)
                })
                .collect_trusted::<Vec<_>>();

            argsort_branch(
                vals.as_mut_slice(),
                reverse,
                |(_, a), (_, b)| order_default_null(a, b),
                |(_, a), (_, b)| order_reverse_null(a, b),
            );
            let ca: NoNull<IdxCa> = vals.into_iter().map(|(idx, _v)| idx).collect_trusted();
            let mut ca = ca.into_inner();
            ca.rename(self.name());
            ca
        } else {
            self.logical.argsort(reverse)
        }
    }

    /// Retrieve the indexes need to sort this and the other arrays.
    fn argsort_multiple(&self, other: &[Series], reverse: &[bool]) -> Result<IdxCa> {
        if self.use_lexical_sort() {
            panic!("lexical sort not yet supported for argsort multiple")
        } else {
            self.logical.argsort_multiple(other, reverse)
        }
    }
}