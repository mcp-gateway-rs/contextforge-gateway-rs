use std::thread;

use contextforge_gateway_rs_lib::{Config, Gateway};
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

    pub fn execute(self, gateway: Gateway) -> contextforge_gateway_rs_lib::Result<()> {
        if self.single_runtime {
            let mut builder = Builder::new_multi_thread();
            self.configure_builder(&mut builder, self.thread_name.clone());
            let runtime = builder.build()?;
            let result = runtime.block_on(gateway.run_gateway());
            if result.is_ok() {
                debug!("Gateway process terminated");
            } else {
                error!("Gateway process terminated {result:?}");
            }
            result
        } else {
            let handles = (0..self.number_of_threads)
                .map(|i| {
                    let thread_name = self.thread_name.clone();
                    let gateway = gateway.clone();
                    thread::Builder::new().name("contextforge-gateway-rs-{i}".to_owned()).spawn(move || {
                        let mut builder = Builder::new_current_thread();

                        Self::configure_single_thread_builder(&mut builder, format!("{thread_name}{i}"));
                        let runtime = match builder.build_local(LocalOptions::default()) {
                            Ok(runtime) => runtime,
                            Err(error) => {
                                warn!("Can't build thread {error:?}");
                                return Err(error.into());
                            },
                        };

                        let result = runtime.block_on(gateway.run_gateway());
                        if result.is_ok() {
                            debug!("Gateway process terminated");
                        } else {
                            error!("Gateway process terminated {result:?}");
                        }
                        result
                    })
                })
                .collect::<Vec<_>>();

            for maybe_handle in handles {
                if let Ok(handle) = maybe_handle {
                    match handle.join() {
                        Ok(Ok(())) => info!("Thread terminated"),
                        Ok(Err(error)) => return Err(error),
                        Err(error) => return Err(format!("Gateway thread panicked: {error:?}").into()),
                    }
                } else {
                    warn!("Thread terminated at start with {maybe_handle:?}");
                }
            }
            Ok(())
        }
    }
}
