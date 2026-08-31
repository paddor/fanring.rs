use crate::error::ChannelError;

pub(crate) fn validate_capacity(capacity: usize, max: usize) -> Result<(), ChannelError> {
    if capacity == 0 {
        return Err(ChannelError::ZeroCapacity);
    }
    if capacity > max {
        return Err(ChannelError::CapacityTooLarge {
            requested: capacity,
            max,
        });
    }
    Ok(())
}
