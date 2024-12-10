pub struct InvalidPointerError;

#[allow(dead_code)]
pub trait SliceReadExt<T> {
    fn read_u8(&self, pointer: &mut usize) -> Result<u8, InvalidPointerError>;
    fn read_u16(&self, pointer: &mut usize) -> Result<u16, InvalidPointerError>;
    fn read_u24(&self, pointer: &mut usize) -> Result<u32, InvalidPointerError>;
    fn read_u32(&self, pointer: &mut usize) -> Result<u32, InvalidPointerError>;
    fn read_u32_le(&self, pointer: &mut usize) -> Result<u32, InvalidPointerError>;
    fn read_bytes(&self, pointer: &mut usize, length: usize) -> Result<&[T], InvalidPointerError>;
}

impl SliceReadExt<u8> for &[u8] {
    fn read_u8(&self, pointer: &mut usize) -> Result<u8, InvalidPointerError> {
        let byte = self.get(*pointer).copied().ok_or(InvalidPointerError)?;
        *pointer += 1;
        Ok(byte)
    }

    fn read_u16(&self, pointer: &mut usize) -> Result<u16, InvalidPointerError> {
        let bytes: &[u8; 2] = self.read_bytes(pointer, 2)?.try_into().unwrap();
        let u16_bytes = [bytes[0], bytes[1]];
        Ok(u16::from_be_bytes(u16_bytes))
    }

    fn read_u24(&self, pointer: &mut usize) -> Result<u32, InvalidPointerError> {
        let bytes: &[u8; 3] = self.read_bytes(pointer, 3)?.try_into().unwrap();
        let u32_bytes = [0, bytes[0], bytes[1], bytes[2]];
        Ok(u32::from_be_bytes(u32_bytes))
    }

    fn read_u32(&self, pointer: &mut usize) -> Result<u32, InvalidPointerError> {
        let bytes = self.read_bytes(pointer, 4)?.try_into().unwrap();
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u32_le(&self, pointer: &mut usize) -> Result<u32, InvalidPointerError> {
        let bytes = self.read_bytes(pointer, 4)?.try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_bytes(&self, pointer: &mut usize, length: usize) -> Result<&[u8], InvalidPointerError> {
        let bytes = self
            .get(*pointer..(*pointer + length))
            .ok_or(InvalidPointerError)?;
        *pointer += length;
        Ok(bytes)
    }
}
