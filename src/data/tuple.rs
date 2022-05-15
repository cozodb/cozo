#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum Tag {
    BoolFalse = 1,
    Null = 2,
    BoolTrue = 3,
    Int = 4,
    Float = 5,
    Text = 6,
    Uuid = 7,

    Bytes = 64,

    List = 128,
    Dict = 129,

    DescVal = 192,

    Max = 255,
}

impl TryFrom<u8> for Tag {
    type Error = u8;
    #[inline]
    fn try_from(u: u8) -> std::result::Result<Tag, u8> {
        use self::Tag::*;
        Ok(match u {
            1 => BoolFalse,
            2 => Null,
            3 => BoolTrue,
            4 => Int,
            5 => Float,
            6 => Text,
            7 => Uuid,

            64 => Bytes,

            128 => List,
            129 => Dict,

            192 => DescVal,

            255 => Max,
            v => return Err(v),
        })
    }
}
