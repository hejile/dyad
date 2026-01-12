struct Promise<T> {
    value: Option<T>,
    error: Option<String>,
    is_resolved: bool,
}

impl<T> Promise<T> {
    pub fn new() -> Self {
        Promise {
            value: None,
            error: None,
            is_resolved: false,
        }
    }

    pub async fn wait_value(&mut self) -> T {
        todo!()
    }
}

struct PromiseResolver<T> {
    promise: Promise<T>,
}
