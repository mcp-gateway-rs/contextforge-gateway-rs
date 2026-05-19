use std::{sync::Arc, thread};

use contextforge_gateway_rs_cpex::CpexRuntimeRegistry;
use contextforge_gateway_rs_lib::{Config, Gateway};
use futures::{
    FutureExt,
    future::{BoxFuture, join_all},
};
use tokio::runtime::{Builder, LocalOptions};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub struct Runtime {
    single_runtime: bool,
    number_of_threads: usize,
    global_queue_interval: Option<u32>,
    event_interval: Option<u32>,
    max_io_events_per_tick: Option<usize>,
    thread_name: String,
}

impl<'b> From<&'b Config> for Runtime {
    fn from(config: &'b Config) -> Self {
        Self {
            single_runtime: config.single_runtime.unwrap_or(true),
            number_of_threads: config.number_of_cpus.unwrap_or(num_cpus::get()),
            ..Default::default()
        }
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            single_runtime: true,
            number_of_threads: num_cpus::get(),
            global_queue_interval: Option::default(),
            event_interval: Option::default(),
            max_io_events_per_tick: Option::default(),
            thread_name: "mcp-gateway-rs-runtime".to_owned(),
        }
    }
}

impl Runtime {
    fn configure_builder(&self, builder: &mut Builder, thread_name: String) {
        let builder = builder.enable_all().name(thread_name);
        let builder = if let Some(global_queue_interval) = self.global_queue_interval {
            builder.global_queue_interval(global_queue_interval)
        } else {
            builder
        };

        if let Some(event_interval) = self.event_interval {
            builder.event_interval(event_interval);
        }

        if let Some(max_io_events_per_tick) = self.max_io_events_per_tick {
            builder.max_io_events_per_tick(max_io_events_per_tick);
        }
    }

    fn configure_single_thread_builder(builder: &mut Builder, thread_name: String) {
        builder.enable_all().name(thread_name).global_queue_interval(1024).max_io_events_per_tick(4);
    }

    pub fn execute(
        self,
        gateway: Gateway,
        cpex_runtime: Option<Arc<CpexRuntimeRegistry>>,
    ) -> contextforge_gateway_rs_lib::Result<()> {
        if self.single_runtime {
            let mut builder = Builder::new_multi_thread();
            self.configure_builder(&mut builder, self.thread_name.clone());
            let runtime = builder.build()?;

            runtime.block_on(async { Self::initialize(gateway, cpex_runtime).await })
        } else {
            let handles = (0..self.number_of_threads)
                .map(|i| {
                    let thread_name = self.thread_name.clone();
                    let gateway = gateway.clone();
                    let cpex_runtime = cpex_runtime.clone();
                    thread::Builder::new().name("contextforge-gateway-rs-{i}".to_owned()).spawn(move || {
                        let mut builder = Builder::new_current_thread();

                        Self::configure_single_thread_builder(&mut builder, format!("{thread_name}{i}"));
                        let runtime = match builder.build_local(LocalOptions::default()) {
                            Ok(runtime) => runtime,
                            Err(error) => {
                                warn!("Can't build thread {error:?}");
                                return Err::<(), contextforge_gateway_rs_lib::Error>(error.into());
                            },
                        };

                        runtime.block_on(async { Self::initialize(gateway, cpex_runtime).await })
                    })
                })
                .collect::<Vec<_>>();

            for maybe_handle in handles {
                if let Ok(handle) = maybe_handle {
                    let res = handle.join();
                    info!("Thread terminated with {res:?}");
                } else {
                    warn!("Thread terminated at start with {maybe_handle:?}");
                }
            }
            Ok(())
        }
    }

    fn initialize(
        gateway: Gateway,
        cpex_runtime: Option<Arc<CpexRuntimeRegistry>>,
    ) -> BoxFuture<'static, contextforge_gateway_rs_lib::Result<()>> {
        async {
            if let Some(cpex_runtime) = cpex_runtime {
                let handle = cpex_runtime.initialize().await;
                match handle {
                    Ok(Some(_runtime_handle)) => {
                        debug!("CPEX Plugins initialization successful");
                    },
                    Ok(None) => {
                        debug!("CPEX Plugins initialization skipped??");
                    },
                    Err(e) => {
                        error!("CPEX Plugins initialization failed {e:?}");
                        return Err(e);
                    },
                }
            }

            let _ = join_all(vec![
                async {
                    let res = gateway.run_gateway().await;
                    if res.is_ok() {
                        debug!("Gateway process terminated");
                    } else {
                        error!("Gateway process terminated {res:?}");
                    }
                }
                .boxed(),
            ])
            .await;

            Ok(())
        }
        .boxed()
    }
}
