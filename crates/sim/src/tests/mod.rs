//! The kit's self-tests: the RNG and clock, the driver (determinism, seed
//! exploration, failure reports, seed selection), and every fault kind,
//! against a toy in-memory bus ([`toy_bus`]), not the real one.

mod bus;
mod clock;
mod driver;
mod every_fault;
mod ids;
mod rng;
mod store;
mod toy_bus;
mod upstream;
