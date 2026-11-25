//! Minimal NIST P-256 (secp256r1) arithmetic for SigV4a ECDSA.
//!
//! Moved from `services/s3/src/auth.rs`; checked against NIST point and RFC 6979
//! vectors in `tests`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct U256(pub [u64; 4]); // big-endian limb order for natural Ord

/// Field prime p = 2^256 - 2^224 + 2^192 + 2^96 - 1.
pub const P: U256 = U256([
    0xFFFF_FFFF_0000_0001,
    0x0000_0000_0000_0000,
    0x0000_0000_FFFF_FFFF,
    0xFFFF_FFFF_FFFF_FFFF,
]);

/// Curve order n.
pub const N: U256 = U256([
    0xFFFF_FFFF_0000_0000,
    0xFFFF_FFFF_FFFF_FFFF,
    0xBCE6_FAAD_A717_9E84,
    0xF3B9_CAC2_FC63_2551,
]);

pub const N_MINUS_2: U256 = U256([
    0xFFFF_FFFF_0000_0000,
    0xFFFF_FFFF_FFFF_FFFF,
    0xBCE6_FAAD_A717_9E84,
    0xF3B9_CAC2_FC63_254F,
]);

const P_MINUS_2: U256 = U256([
    0xFFFF_FFFF_0000_0001,
    0x0000_0000_0000_0000,
    0x0000_0000_FFFF_FFFF,
    0xFFFF_FFFF_FFFF_FFFD,
]);

const GX: U256 = U256([
    0x6B17_D1F2_E12C_4247,
    0xF8BC_E6E5_63A4_40F2,
    0x7703_7D81_2DEB_33A0,
    0xF4A1_3945_D898_C296,
]);

const GY: U256 = U256([
    0x4FE3_42E2_FE1A_7F9B,
    0x8EE7_EB4A_7C0F_9E16,
    0x2BCE_3357_6B31_5ECE,
    0xCBB6_4068_37BF_51F5,
]);

pub const ZERO: U256 = U256([0, 0, 0, 0]);
pub const ONE: U256 = U256([0, 0, 0, 1]);

impl U256 {
    pub fn from_be_bytes(bytes: &[u8; 32]) -> Self {
        U256([
            u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
            u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
        ])
    }

    pub fn to_be_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[0..8].copy_from_slice(&self.0[0].to_be_bytes());
        out[8..16].copy_from_slice(&self.0[1].to_be_bytes());
        out[16..24].copy_from_slice(&self.0[2].to_be_bytes());
        out[24..32].copy_from_slice(&self.0[3].to_be_bytes());
        out
    }

    pub fn is_zero(self) -> bool {
        self == ZERO
    }

    pub fn add_one(self) -> Self {
        let (out, _) = add_raw(self, ONE);
        out
    }

    fn bit(self, index: usize) -> bool {
        let limb = 3 - (index / 64);
        ((self.0[limb] >> (index % 64)) & 1) != 0
    }
}

fn add_raw(a: U256, b: U256) -> (U256, bool) {
    let mut out = [0u64; 4];
    let mut carry = 0u128;
    for i in (0..4).rev() {
        let sum = a.0[i] as u128 + b.0[i] as u128 + carry;
        out[i] = sum as u64;
        carry = sum >> 64;
    }
    (U256(out), carry != 0)
}

fn sub_raw(a: U256, b: U256) -> (U256, bool) {
    let mut out = [0u64; 4];
    let mut borrow = 0i128;
    for i in (0..4).rev() {
        let diff = a.0[i] as i128 - b.0[i] as i128 - borrow;
        if diff < 0 {
            out[i] = (diff + (1i128 << 64)) as u64;
            borrow = 1;
        } else {
            out[i] = diff as u64;
            borrow = 0;
        }
    }
    (U256(out), borrow != 0)
}

pub fn add_mod(a: U256, b: U256, m: U256) -> U256 {
    let (sum, carry) = add_raw(a, b);
    if carry || sum >= m {
        sub_raw(sum, m).0
    } else {
        sum
    }
}

pub fn sub_mod(a: U256, b: U256, m: U256) -> U256 {
    if a >= b {
        sub_raw(a, b).0
    } else {
        let (diff, _) = sub_raw(b, a);
        sub_raw(m, diff).0
    }
}

pub fn reduce_mod(mut a: U256, m: U256) -> U256 {
    while a >= m {
        a = sub_raw(a, m).0;
    }
    a
}

pub fn mul_mod(a: U256, b: U256, m: U256) -> U256 {
    let a = reduce_mod(a, m);
    let mut acc = ZERO;
    for i in (0..256).rev() {
        acc = add_mod(acc, acc, m);
        if b.bit(i) {
            acc = add_mod(acc, a, m);
        }
    }
    acc
}

pub fn pow_mod(mut base: U256, exp: U256, m: U256) -> U256 {
    base = reduce_mod(base, m);
    let mut result = ONE;
    for i in (0..256).rev() {
        result = mul_mod(result, result, m);
        if exp.bit(i) {
            result = mul_mod(result, base, m);
        }
    }
    result
}

fn inv_p(a: U256) -> U256 {
    pow_mod(a, P_MINUS_2, P)
}

pub fn inv_n(a: U256) -> U256 {
    pow_mod(a, N_MINUS_2, N)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AffinePoint {
    pub x: U256,
    pub y: U256,
    pub infinity: bool,
}

impl AffinePoint {
    pub const INFINITY: Self = AffinePoint {
        x: ZERO,
        y: ZERO,
        infinity: true,
    };
}

#[derive(Debug, Clone, Copy)]
struct JacobianPoint {
    x: U256,
    y: U256,
    z: U256,
}

impl JacobianPoint {
    const INFINITY: Self = JacobianPoint {
        x: ONE,
        y: ONE,
        z: ZERO,
    };

    fn from_affine(p: AffinePoint) -> Self {
        if p.infinity {
            Self::INFINITY
        } else {
            JacobianPoint {
                x: p.x,
                y: p.y,
                z: ONE,
            }
        }
    }

    fn to_affine(self) -> AffinePoint {
        if self.z.is_zero() {
            return AffinePoint::INFINITY;
        }
        let z_inv = inv_p(self.z);
        let z_inv2 = mul_mod(z_inv, z_inv, P);
        let z_inv3 = mul_mod(z_inv2, z_inv, P);
        AffinePoint {
            x: mul_mod(self.x, z_inv2, P),
            y: mul_mod(self.y, z_inv3, P),
            infinity: false,
        }
    }

    fn double(self) -> Self {
        if self.z.is_zero() || self.y.is_zero() {
            return Self::INFINITY;
        }
        // a = -3 mod p => M = 3*(X - Z^2)*(X + Z^2)
        let z2 = mul_mod(self.z, self.z, P);
        let x_minus_z2 = sub_mod(self.x, z2, P);
        let x_plus_z2 = add_mod(self.x, z2, P);
        let t = mul_mod(x_minus_z2, x_plus_z2, P);
        let m = add_mod(add_mod(t, t, P), t, P);

        let y2 = mul_mod(self.y, self.y, P);
        let xy2 = mul_mod(self.x, y2, P);
        let s = add_mod(add_mod(xy2, xy2, P), add_mod(xy2, xy2, P), P);

        let m2 = mul_mod(m, m, P);
        let two_s = add_mod(s, s, P);
        let x3 = sub_mod(m2, two_s, P);

        let y4 = mul_mod(y2, y2, P);
        let two_y4 = add_mod(y4, y4, P);
        let four_y4 = add_mod(two_y4, two_y4, P);
        let eight_y4 = add_mod(four_y4, four_y4, P);
        let y3 = sub_mod(mul_mod(m, sub_mod(s, x3, P), P), eight_y4, P);

        let yz = mul_mod(self.y, self.z, P);
        let z3 = add_mod(yz, yz, P);
        JacobianPoint {
            x: x3,
            y: y3,
            z: z3,
        }
    }

    fn add(self, other: Self) -> Self {
        if self.z.is_zero() {
            return other;
        }
        if other.z.is_zero() {
            return self;
        }
        let z1z1 = mul_mod(self.z, self.z, P);
        let z2z2 = mul_mod(other.z, other.z, P);
        let u1 = mul_mod(self.x, z2z2, P);
        let u2 = mul_mod(other.x, z1z1, P);
        let s1 = mul_mod(self.y, mul_mod(other.z, z2z2, P), P);
        let s2 = mul_mod(other.y, mul_mod(self.z, z1z1, P), P);
        if u1 == u2 {
            if s1 == s2 {
                return self.double();
            } else {
                return Self::INFINITY;
            }
        }
        let h = sub_mod(u2, u1, P);
        let r = sub_mod(s2, s1, P);
        let h2 = mul_mod(h, h, P);
        let h3 = mul_mod(h2, h, P);
        let u1h2 = mul_mod(u1, h2, P);
        let x3 = sub_mod(sub_mod(mul_mod(r, r, P), h3, P), add_mod(u1h2, u1h2, P), P);
        let y3 = sub_mod(mul_mod(r, sub_mod(u1h2, x3, P), P), mul_mod(s1, h3, P), P);
        let z3 = mul_mod(h, mul_mod(self.z, other.z, P), P);
        JacobianPoint {
            x: x3,
            y: y3,
            z: z3,
        }
    }
}

pub fn scalar_mul(point: AffinePoint, scalar: &U256) -> AffinePoint {
    if point.infinity || scalar.is_zero() {
        return AffinePoint::INFINITY;
    }
    let base = JacobianPoint::from_affine(point);
    let mut acc = JacobianPoint::INFINITY;
    for i in (0..256).rev() {
        acc = acc.double();
        if scalar.bit(i) {
            acc = acc.add(base);
        }
    }
    acc.to_affine()
}

pub fn scalar_mul_g(scalar: &U256) -> AffinePoint {
    scalar_mul(
        AffinePoint {
            x: GX,
            y: GY,
            infinity: false,
        },
        scalar,
    )
}

pub fn ecdsa_verify(pub_key: &AffinePoint, z: &U256, r: &U256, s: &U256) -> bool {
    if pub_key.infinity || r.is_zero() || *r >= N || s.is_zero() || *s >= N {
        return false;
    }
    let w = inv_n(*s);
    let u1 = mul_mod(*z, w, N);
    let u2 = mul_mod(*r, w, N);
    let p1 = JacobianPoint::from_affine(scalar_mul_g(&u1));
    let p2 = JacobianPoint::from_affine(scalar_mul(*pub_key, &u2));
    let point = p1.add(p2).to_affine();
    if point.infinity {
        return false;
    }
    reduce_mod(point.x, N) == *r
}

/// ECDSA P-256 signing with an explicit nonce `k`, producing ASN.1 DER.
pub fn ecdsa_sign_der_with_k(private_key: &U256, z: &U256, k: &U256) -> Vec<u8> {
    let r_point = scalar_mul_g(k);
    let r = reduce_mod(r_point.x, N);
    let k_inv = inv_n(*k);
    let rd = mul_mod(r, *private_key, N);
    let s = mul_mod(k_inv, add_mod(reduce_mod(*z, N), rd, N), N);
    encode_der_sig(&r, &s)
}

pub fn encode_der_sig(r: &U256, s: &U256) -> Vec<u8> {
    let r_enc = encode_der_uint(r);
    let s_enc = encode_der_uint(s);
    let mut out = Vec::with_capacity(2 + r_enc.len() + s_enc.len());
    out.push(0x30);
    out.push((r_enc.len() + s_enc.len()) as u8);
    out.extend_from_slice(&r_enc);
    out.extend_from_slice(&s_enc);
    out
}

fn encode_der_uint(v: &U256) -> Vec<u8> {
    let bytes = v.to_be_bytes();
    let first_nz = bytes.iter().position(|&b| b != 0).unwrap_or(31);
    let slice = &bytes[first_nz..];
    let needs_pad = slice[0] & 0x80 != 0;
    let mut out = Vec::with_capacity(2 + slice.len() + usize::from(needs_pad));
    out.push(0x02);
    out.push((slice.len() + usize::from(needs_pad)) as u8);
    if needs_pad {
        out.push(0x00);
    }
    out.extend_from_slice(slice);
    out
}
