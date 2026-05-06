use contextforge_gateway_rs_lib::{Config, Gateway};
use std::thread;
use tokio::runtime::{Builder, LocalOptions};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
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

    fn configure_single_thread_builder(&self, builder: &mut Builder, thread_name: String) {
        builder.enable_all().name(thread_name).global_queue_interval(1024).max_io_events_per_tick(4);
    }

    pub fn execute(self, gateway: Gateway) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        if self.single_runtime {
            let mut builder = Builder::new_multi_thread();
            self.configure_builder(&mut builder, self.thread_name.to_owned());
            let runtime = builder.build()?;
            runtime.block_on(async {
                tokio::select! {
                    res = gateway.run_gateway() =>
                        if res.is_ok(){
                            debug!("Gateway process terminated");
                        }else{
                            error!("Gateway process terminated {res:?}");
                        }
                }
            });
            Ok(())
        } else {
            let handles = (0..self.number_of_threads)
                .map(|i| {
                    let thread_name = self.thread_name.clone();
                    let runtime = self.clone();
                    let gateway = gateway.clone();
                    // let config = config.clone();
                    // let session_manager = session_manager.clone();
                    thread::Builder::new().name("contextforge-gateway-rs-{i}".to_owned()).spawn(move || {
                        let mut builder = Builder::new_current_thread();

                        runtime.configure_single_thread_builder(&mut builder, format!("{}{}", thread_name, i));
                        let maybe_runtime = builder.build_local(LocalOptions::default());
                        let Ok(runtime) = maybe_runtime else {
                            warn!("Can't build thread {maybe_runtime:?}");
                            return Err(maybe_runtime.err());
                        };

                        runtime.block_on(async {
                            tokio::select! {
                                res = gateway.run_gateway() =>
                                    if res.is_ok(){
                                        debug!("Gateway process terminated");
                                    }else{
                                        error!("Gateway process terminated {res:?}");
                                    }
                            }
                        });
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();

            handles.into_iter().for_each(|maybe_handle| {
                if let Ok(handle) = maybe_handle {
                    let res = handle.join();
                    info!("Thread terminated with {res:?}");
                } else {
                    warn!("Thread terminated at start with {maybe_handle:?}");
                }
            });
            Ok(())
        }
    }
}
