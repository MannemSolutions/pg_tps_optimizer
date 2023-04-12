use num::Integer;

pub struct Fibonacci<T: Integer> {
    curr: T,
    next: T,
}

impl <T: Integer> Iterator for Fibonacci<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        let new_next = self.curr + self.next;

        self.curr = self.next;
        self.next = new_next;

        Some(self.curr)
    }
}

impl <T: Integer> Fibonacci<T> {
    pub fn new(curr: T, next: T) -> Fibonacci<T> {
        Fibonacci{
            curr,
            next,
        }
    }
    pub fn list(curr: T, next: T, count: usize) -> Vec<T> {
        Fibonacci::new(curr, next).take(count).collect()
    }
    pub fn value(curr: T, next: T, number: usize) -> T {
        Fibonacci::new(curr, next).take(number).last().unwrap()
    }
}
