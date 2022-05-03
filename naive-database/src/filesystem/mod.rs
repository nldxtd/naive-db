pub mod file_manager;

#[cfg(not(feature = "mmap"))]
#[path = "page_manager.rs"]
pub mod page_manager;

#[cfg(feature = "mmap")]
#[path = "mmap.rs"]
pub mod page_manager;

#[cfg(test)]
mod tests;
