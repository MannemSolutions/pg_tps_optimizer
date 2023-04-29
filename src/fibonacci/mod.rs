pub struct Fibonacci {
    curr: u32,
    next: u32,
}

impl Iterator for Fibonacci {
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item> {
        let new_next = self.curr + self.next;

        self.curr = self.next;
        self.next = new_next;

        Some(self.curr)
    }
}

impl Fibonacci {
    pub fn new(curr: u32, next: u32) -> Fibonacci {
        Fibonacci{
            curr,
            next,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fibonacci() {
        assert_eq!(Fibonacci::new(1,1)
                   .take(5)
                   .collect::<Vec<u32>>()
                   .len(), 5);
        let sum: u32 = Fibonacci::new(1,1)
                   .take(5)
                   .collect::<Vec<u32>>()
                   .iter()
                   .sum();
        assert_eq!(sum, 19);
        assert_eq!(Fibonacci::new(1,1)
                   .take(5)
                   .last()
                   .unwrap(), 8);
    }
}
