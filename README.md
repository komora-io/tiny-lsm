# tiny-lsm

Super simple in-memory blocking LSM for constant-size keys and values.

Despite being single-threaded and blocking, this is still capable
of outperforming a wide range of other storage systems.

This is a great choice when you:
* want to fit the whole data set in-memory
* can model your keys and values in a bounded number of bytes

Tested with [fuzzcheck](https://docs.rs/fuzzcheck), and the API and
internals are intentionally being kept minimal to reduce bugs and
improve performance for the use cases that this works well for.

Pairs extremely well with the [zerocopy](https://docs.rs/zerocopy)
crate for viewing the fixed size byte arrays as typed data without
paying expensive deserialization costs.
