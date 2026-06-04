#![cfg_attr(target_arch = "wasm32", no_std)]

#[cfg(target_arch = "wasm32")]
extern crate alloc;

wit_bindgen::generate!({
    path: "../../wit",
    world: "pureflow-node",
});

#[cfg(target_arch = "wasm32")]
use alloc::{
    string::String,
    vec,
    vec::Vec,
};
#[cfg(target_arch = "wasm32")]
use core::{
    alloc::{GlobalAlloc, Layout},
    panic::PanicInfo,
    ptr,
};
#[cfg(not(target_arch = "wasm32"))]
use std::{
    string::String,
    vec,
    vec::Vec,
};

use exports::pureflow::batch::batch::{BatchError, Guest, Payload, PortBatch};

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOCATOR: BumpAllocator = BumpAllocator;

#[cfg(target_arch = "wasm32")]
const WASM_PAGE_SIZE: usize = 64 * 1024;

#[cfg(target_arch = "wasm32")]
static mut HEAP_NEXT: usize = 0;

#[cfg(target_arch = "wasm32")]
unsafe extern "C" {
    static __heap_base: u8;
}

#[cfg(target_arch = "wasm32")]
struct BumpAllocator;

#[cfg(target_arch = "wasm32")]
unsafe impl GlobalAlloc for BumpAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some(align) = normalize_align(layout.align()) else {
            return ptr::null_mut();
        };
        let size = layout.size();
        if size == 0 {
            return align as *mut u8;
        }

        let heap_next = unsafe {
            if HEAP_NEXT == 0 {
                HEAP_NEXT = heap_base();
            }
            HEAP_NEXT
        };
        let Some(aligned) = align_up(heap_next, align) else {
            return ptr::null_mut();
        };
        let Some(next) = aligned.checked_add(size) else {
            return ptr::null_mut();
        };
        if !grow_memory_to(next) {
            return ptr::null_mut();
        }
        unsafe {
            HEAP_NEXT = next;
        }

        aligned as *mut u8
    }

    unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {}

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size == 0 {
            return layout.align() as *mut u8;
        }
        let new_ptr =
            unsafe { self.alloc(Layout::from_size_align_unchecked(new_size, layout.align())) };
        if !new_ptr.is_null() {
            unsafe {
                ptr::copy_nonoverlapping(ptr, new_ptr, layout.size().min(new_size));
            }
        }
        new_ptr
    }
}

#[cfg(target_arch = "wasm32")]
fn align_up(value: usize, align: usize) -> Option<usize> {
    value
        .checked_add(align.checked_sub(1)?)
        .map(|value| value & !(align - 1))
}

#[cfg(target_arch = "wasm32")]
fn normalize_align(align: usize) -> Option<usize> {
    let align = align.max(1);
    if align.is_power_of_two() {
        Some(align)
    } else {
        align.checked_next_power_of_two()
    }
}

#[cfg(target_arch = "wasm32")]
fn heap_base() -> usize {
    ptr::addr_of!(__heap_base) as usize
}

#[cfg(target_arch = "wasm32")]
fn grow_memory_to(required_end: usize) -> bool {
    let current_pages = core::arch::wasm32::memory_size(0);
    let current_size = current_pages.saturating_mul(WASM_PAGE_SIZE);
    if required_end <= current_size {
        return true;
    }

    let Some(additional_bytes) = required_end.checked_sub(current_size) else {
        return false;
    };
    let additional_pages = additional_bytes.div_ceil(WASM_PAGE_SIZE);
    core::arch::wasm32::memory_grow(0, additional_pages) != usize::MAX
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
unsafe extern "C" fn cabi_realloc(
    old_ptr: *mut u8,
    old_len: usize,
    align: usize,
    new_len: usize,
) -> *mut u8 {
    let Some(align) = normalize_align(align) else {
        return ptr::null_mut();
    };
    if old_len == 0 {
        if new_len == 0 {
            return align as *mut u8;
        }
        let layout = unsafe { Layout::from_size_align_unchecked(new_len, align) };
        return unsafe { ALLOCATOR.alloc(layout) };
    }

    let layout = unsafe { Layout::from_size_align_unchecked(old_len, align) };
    unsafe { ALLOCATOR.realloc(old_ptr, layout, new_len) }
}

#[cfg(target_arch = "wasm32")]
#[panic_handler]
fn panic(_info: &PanicInfo<'_>) -> ! {
    core::arch::wasm32::unreachable()
}

struct UppercaseGuest;

impl Guest for UppercaseGuest {
    fn invoke(inputs: Vec<PortBatch>) -> Result<Vec<PortBatch>, BatchError> {
        let mut packets = Vec::new();

        for input in inputs {
            for mut packet in input.packets {
                let Payload::Bytes(bytes) = packet.payload else {
                    return Err(BatchError::UnsupportedPayload(
                        String::from("uppercase guest accepts only bytes payloads"),
                    ));
                };
                packet.payload = Payload::Bytes(uppercase_ascii(bytes));
                packets.push(packet);
            }
        }

        Ok(vec![PortBatch {
            port_id: String::from("out"),
            packets,
        }])
    }
}

fn uppercase_ascii(mut bytes: Vec<u8>) -> Vec<u8> {
    for byte in &mut bytes {
        *byte = byte.to_ascii_uppercase();
    }
    bytes
}

export!(UppercaseGuest);
