//! buddy-gateway — a multi-platform IM channel bridge for AI agents and bots.
//!
//! The crate is split into a platform-agnostic
//! **core** (models, constraints, media, group context, the shared channel
//! pipeline, and the manager) and a **channel adapter layer** (`channels`)
//! that adapts each IM platform to the common [`channel::Channel`] contract.
//!
//! See `docs/DESIGN.md` for the architecture overview.

pub mod channel;
pub mod constraints;
pub mod errors;
pub mod group_context;
pub mod manager;
pub mod media;
pub mod models;
pub mod push_routing;
pub mod utils;

pub mod channels;

pub use channel::{Channel, ChannelConfig, ChannelConfigLike, MessageProcessor};
pub use errors::{GatewayError, Result};
pub use manager::{ChannelManager, ChannelOptions, MessagePredicate, PreLockHandler};
