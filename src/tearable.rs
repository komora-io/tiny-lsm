use std::io::{self, Write};

/// A writer that can facilitate corruption and torn writes
/// for testing purposes.
pub struct Tearable<W: Write> {
    inner: W,
    buffer: Vec<u8>,
    pub tearing: bool,
}

impl<W: Write> Tearable<W> {
    pub fn new(inner: W) -> Self {
        Tearable {
            inner,
            buffer: vec![],
            tearing: false,
        }
    }

    pub fn begin_tear(&mut self) {
        assert!(!self.tearing);
        self.tearing = true;
    }

    pub fn apply_tear(&mut self, offset: usize, corrupt: bool) {
        assert!(self.tearing);

        if self.buffer.is_empty() {
            return;
        }

        let at = offset % self.buffer.len();

        if corrupt {
            log::debug!(
                "corrupting {} to {} at idx {}",
                self.buffer[at],
                self.buffer[at] ^ 0xFF,
                at
            );

            self.buffer[at] ^= 0xFF;
        } else {
            self.buffer.truncate(at);
        }
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }
}

impl<W: Write> Write for Tearable<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.write(&buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.tearing {
            Ok(())
        } else {
            self.inner.write_all(&self.buffer)?;
            self.inner.flush()?;
            self.buffer.clear();
            Ok(())
        }
    }
}
