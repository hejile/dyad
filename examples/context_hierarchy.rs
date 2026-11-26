use dyad::{IntoTaskGroup, test_runtime::TestRuntime};

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

struct AppRoot;

impl IntoTaskGroup<Self, AppContext> for AppRoot {
    fn into_task_group(self) -> (Self, impl FnOnce(&mut dyad::DyadSpawner<Self, AppContext>)) {
        (self, |spawner| {
            spawner.spawn_task(async |dh| {
                let context = dh.context();
                context.logger.log(&format!(
                    "{} v{}\n",
                    context.app_state.title, context.app_state.version,
                ));
            });
            spawner.spawn_task_group_with_context(m1::Module1Root, m1::Module1Context::new());
        })
    }
    
    type Handle = ();
    
    const SHUTDOWN_ON_LAST_HANDLE_DROP: bool = false;
}

mod m1 {
    use dyad::{ChildDyadContext, IntoTaskGroup, ParentContextHolder};

    use crate::AppContext;
    use std::{cell::RefCell, ops::Deref, rc::Rc};

    pub(super) struct Module1Context {
        state: Module1State,
        parent: ParentContextHolder<AppContext>,
    }

    impl Module1Context {
        pub fn new() -> Self {
            Self {
                state: Module1State {
                    new_requests: Default::default(),
                },
                parent: Default::default(),
            }
        }
    }

    impl Deref for Module1Context {
        type Target = AppContext;

        fn deref(&self) -> &Self::Target {
            &self.parent
        }
    }

    impl ChildDyadContext for Module1Context {
        type Parent = AppContext;

        fn parent_holder_mut(&mut self) -> &mut ParentContextHolder<Self::Parent> {
            &mut self.parent
        }
    }

    struct Module1State {
        new_requests: Rc<RefCell<Vec<String>>>,
    }

    impl Module1State {
        pub fn add_request(&self, s: String) {
            self.new_requests.borrow_mut().push(s);
        }
    }

    pub(super) struct Module1Root;

    impl IntoTaskGroup<Self, Module1Context> for Module1Root {
        fn into_task_group(
            self,
        ) -> (
            Self,
            impl FnOnce(&mut dyad::DyadSpawner<Self, Module1Context>),
        ) {
            (self, |spawner| {
                spawner.spawn_task(async |dh| {
                    dh.context().logger.log("m1 hello\n");
                });
                spawner.spawn_task_group_with_context(m2::Module2Root, m2::Module2Context::new());
            })
        }
        
        type Handle = ();
        
        const SHUTDOWN_ON_LAST_HANDLE_DROP: bool = false;
    }

    mod m2 {
        use dyad::{ChildDyadContext, IntoTaskGroup, ParentContextHolder};
        use std::{cell::Cell, ops::Deref};

        use super::Module1Context;

        pub(super) struct Module2Context {
            state: Module2State,
            parent: ParentContextHolder<Module1Context>,
        }

        impl Module2Context {
            pub(super) fn new() -> Self {
                Self {
                    state: Module2State {
                        counter: Cell::new(0),
                    },
                    parent: Default::default(),
                }
            }
        }

        impl Deref for Module2Context {
            type Target = Module1Context;

            fn deref(&self) -> &Self::Target {
                &self.parent
            }
        }

        impl ChildDyadContext for Module2Context {
            type Parent = Module1Context;

            fn parent_holder_mut(&mut self) -> &mut ParentContextHolder<Self::Parent> {
                &mut self.parent
            }
        }

        struct Module2State {
            counter: Cell<usize>,
        }

        impl Module2State {
            pub fn add_counter(&self, n: usize) {
                self.counter.set(self.counter.get() + n)
            }
        }

        pub(super) struct Module2Root;

        impl IntoTaskGroup<Self, Module2Context> for Module2Root {
            fn into_task_group(
                self,
            ) -> (
                Self,
                impl FnOnce(&mut dyad::DyadSpawner<Self, Module2Context>),
            ) {
                (self, |spawner| {
                    spawner.spawn_task(async |dh| {
                        let context = dh.context();
                        let parent: &Module1Context = context;
                        parent.state.add_request(String::from("request from m2"));
                        context.state.add_counter(1);
                        context.logger.log(&format!(
                            "m2 hello (counter: {})\n",
                            context.state.counter.get()
                        ));
                    });
                })
            }
            
            type Handle = ();
            
            const SHUTDOWN_ON_LAST_HANDLE_DROP: bool = false;
        }
    }
}

pub fn main() {
    let mut rt = TestRuntime::new();
    let root = AppRoot;
    rt.spawn_task_group(root, AppContext::new());
    rt.run();
}
