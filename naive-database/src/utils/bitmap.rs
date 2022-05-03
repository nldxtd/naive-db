#[inline]
pub fn bit_at(bitmap: &[u8], i: usize) -> bool {
    let u = bitmap[i / 8];
    u & (1 << (i % 8)) != 0
}

#[inline]
pub fn set_bit_at(bitmap: &mut [u8], i: usize) {
    bitmap[i / 8] |= 1 << (i % 8);
}

#[inline]
pub fn toggle_bit_at(bitmap: &mut [u8], i: usize) {
    bitmap[i / 8] ^= 1 << (i % 8);
}

#[inline]
pub fn clear_bit_at(bitmap: &mut [u8], i: usize) {
    bitmap[i / 8] &= !(1 << (i % 8));
}

#[inline]
pub fn iter_bits(bitmap: &[u8]) -> impl Iterator<Item = bool> + '_ {
    (0..bitmap.len()).map(move |i| bit_at(bitmap, i))
}
