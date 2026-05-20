use std::{
    sync::{Arc, mpsc},
    thread,
};

use contextforge_gateway_rs_cpex::CpexRuntimeRegistry;
use contextforge_gateway_rs_lib::{Config, Gateway};
use tokio::runtime::{Builder, LocalOptions};
use tokio::task::JoinHandle;
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

            runtime.block_on(async {
                let _cpex_watcher = Self::initialize_cpex_runtime(cpex_runtime).await?;
                Self::run_gateway(gateway).await
            })
        } else {
            let (init_sender, init_receiver) = mpsc::channel();
            let mut handles = vec![Self::spawn_gateway_thread(
                format!("{}0", self.thread_name),
                gateway.clone(),
                cpex_runtime,
                Some(init_sender),
            )?];
            match init_receiver
                .recv()
                .map_err(|error| format!("CPEX plugin initialization result unavailable: {error}"))?
            {
                Ok(()) => {},
                Err(error) => return Err(error.into()),
            }

            for i in 1..self.number_of_threads {
                match Self::spawn_gateway_thread(format!("{}{i}", self.thread_name), gateway.clone(), None, None) {
                    Ok(handle) => handles.push(handle),
                    Err(error) => warn!("Thread terminated at start with {error:?}"),
                }
            }

            for handle in handles {
                let res = handle.join();
                info!("Thread terminated with {res:?}");
            }
            Ok(())
        }
    }

    fn spawn_gateway_thread(
        thread_name: String,
        gateway: Gateway,
        cpex_runtime: Option<Arc<CpexRuntimeRegistry>>,
        init_sender: Option<mpsc::Sender<std::result::Result<(), String>>>,
    ) -> std::io::Result<thread::JoinHandle<contextforge_gateway_rs_lib::Result<()>>> {
        thread::Builder::new().name(thread_name.clone()).spawn(move || {
            let mut builder = Builder::new_current_thread();
            Self::configure_single_thread_builder(&mut builder, thread_name);
            let runtime = match builder.build_local(LocalOptions::default()) {
                Ok(runtime) => runtime,
                Err(error) => {
                    warn!("Can't build thread {error:?}");
                    return Err::<(), contextforge_gateway_rs_lib::Error>(error.into());
                },
            };

            runtime.block_on(async {
                let Some(init_sender) = init_sender else {
                    return Self::run_gateway(gateway).await;
                };
                match Self::initialize_cpex_runtime(cpex_runtime).await {
                    Ok(cpex_watcher) => {
                        let _ = init_sender.send(Ok(()));
                        let _cpex_watcher = cpex_watcher;
                        Self::run_gateway(gateway).await
                    },
                    Err(error) => {
                        let _ = init_sender.send(Err(error.to_string()));
                        Err(error)
                    },
                }
            })
        })
    }

    async fn initialize_cpex_runtime(
        cpex_runtime: Option<Arc<CpexRuntimeRegistry>>,
    ) -> contextforge_gateway_rs_lib::Result<Option<JoinHandle<()>>> {
        let Some(cpex_runtime) = cpex_runtime else {
            return Ok(None);
        };
        match cpex_runtime.initialize().await {
            Ok(Some(handle)) => {
                debug!("CPEX Plugins initialization successful");
                Ok(Some(handle))
            },
            Ok(None) => {
                debug!("CPEX Plugins initialization skipped");
                Ok(None)
            },
            Err(e) => {
                error!("CPEX Plugins initialization failed {e:?}");
                Err(e)
            },
        }
    }

    async fn run_gateway(gateway: Gateway) -> contextforge_gateway_rs_lib::Result<()> {
        let res = gateway.run_gateway().await;
        if res.is_ok() {
            debug!("Gateway process terminated");
        } else {
            error!("Gateway process terminated {res:?}");
        }
        Ok(())
    }
}
