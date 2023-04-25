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
    pub fn list(curr: u32, next: u32, count: usize) -> Vec<u32> {
        Fibonacci::new(curr, next).take(count).collect()
    }
    pub fn value(curr: u32, next: u32, number: usize) -> u32 {
        Fibonacci::new(curr, next).take(number).last().unwrap()
    }
}
