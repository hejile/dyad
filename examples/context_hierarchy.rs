use dyad::{context::DyadContext, task_group::TaskGroup, test_runtime::TestRuntime};

struct AppContext {
    logger: Logger,
    app_state: AppState,
}

impl AppContext {
    fn new() -> Self {
        Self {
            logger: Logger {},
            app_state: AppState {
                title: "test context hierarchy".to_string(),
                version: 1,
            },
        }
    }
}

struct Logger {}

impl Logger {
    fn log(&self, s: &str) {
        print!("{}", s);
    }
}

struct AppState {
    title: String,
    version: u32,
}

mod m1 {
    use dyad::{context::DyadContext, task_group::TaskGroup};

use crate::AppContext;
    use std::{cell::RefCell, ops::Deref, rc::Rc};

    struct Module1Context {
        state: Module1State,
        parent: Rc<AppContext>,
    }

    impl Deref for Module1Context {
        type Target = AppContext;

        fn deref(&self) -> &Self::Target {
            &self.parent
        }
    }

    struct Module1State {
        new_requests: Rc::<RefCell::<Vec<String>>>,
    }

    impl Module1State {
        pub fn add_request(&self, s: String) {
            self.new_requests.borrow_mut().push(s);
        }
    }

    pub fn m1_tg(pc: Rc<AppContext>) -> TaskGroup<(), Module1Context> {
            let c = Module1Context {
                state: Module1State { new_requests: Default::default() },
                parent: pc,
            };
            let mut tg = TaskGroup::new((), DyadContext::new(c));
            tg.spawn(async |dh| {
                dh.with_context_mut(|c| {
                    c.logger.log("hello");
                })
            });
            tg
    }

    mod m2 {
        use dyad::{context::DyadContext, task_group::TaskGroup};
        use std::{cell::{Cell, RefCell}, ops::Deref, rc::Rc};

        use crate::m1::Module1Context;

        struct Module2Context {
            state: Module2State,
            parent: Rc<Module1Context>,
        }

        impl Deref for Module2Context {
            type Target = Module1Context;

            fn deref(&self) -> &Self::Target {
                &self.parent
            }
        }

        struct Module2State {
            counter: Cell<usize>,
        }

        impl Module2State {
            pub fn add_counter(&self, n : usize) {
                self.counter.set(self.counter.get() + n)
            }
        }

        pub fn m2_tg(pc: Rc<Module1Context>) -> TaskGroup<(), Module2Context> {
            let c = Module2Context {
                state: Module2State { counter: Default::default() },
                parent: pc,
            };
            let mut tg = TaskGroup::new((), DyadContext::new(c));
            tg.spawn(async |dh| {
                dh.with_context_mut(|c| {
                    c.logger.log("hello");
                })
            });
            tg
        }
    }
}

pub fn main() {
    let mut rt = TestRuntime::new();
    let mut root_task_group = TaskGroup::new((), DyadContext::new(AppContext::new()));
    rt.spawn_task_group(root_task_group);
}
