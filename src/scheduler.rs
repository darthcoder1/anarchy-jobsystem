
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;

pub struct JobFence {
    signaled : AtomicBool,
}

pub struct JobScheduler{
    threads : Vec<thread::JoinHandle<()>>,
    pending_jobs : Vec<JobInfo>,
    dispatcher : Arc<JobDispatcher>
}

struct JobInfo {
    dependencies : Vec<Arc<JobFence>>,
    func : Box<Fn()>,
    out_fence : Arc<JobFence>,
}


struct JobDispatcher {
    ready_to_run : Vec<JobInfo>,
    lock : Mutex<i32>
}

impl JobDispatcher {

    fn new() -> JobDispatcher {
        JobDispatcher {
            ready_to_run: vec![],
            lock: Mutex::new(0),
        }
    }

    fn push_job_to_run(& mut self, job : JobInfo) {
        let _guard = self.lock();
        self.ready_to_run.push(job);
    }

    fn pull_job_to_run(& mut self) -> Result<JobInfo, ()> {
        let _guard = self.lock();
        
        if self.ready_to_run.len() > 0 {
            Ok(self.ready_to_run.pop().unwrap())
        } else {
            Err(())
        }
    }
}

pub fn initialize_scheduler(num_job_threads : i32) -> JobScheduler {

    let mut sched = JobScheduler {
        threads : vec![],
        pending_jobs : vec![],
        dispatcher: Arc::new(JobDispatcher::new()),
    };

    for i in 0..num_job_threads {
        let dispatcher = sched.dispatcher.clone();
        sched.threads.push(thread::spawn(move || { scheduler_thread_function(dispatcher, i); } ));
    }

    sched
}

pub fn shutdown_scheduler(sched : JobScheduler) {

    for thread in sched.threads {
        thread.join().unwrap();
    }
}

impl JobScheduler {

    pub fn schedule_job(& mut self, dependencies : & [Arc<JobFence>], job_func : Box<Fn()>) -> Arc<JobFence> {
        let job_info = JobInfo {
            dependencies: dependencies.to_vec(),
            func: job_func,
            out_fence: Arc::new(JobFence { signaled: AtomicBool::from(false) }),
        };

        let out_fence = job_info.out_fence.clone();
        self.pending_jobs.push(job_info);
        
        out_fence
    }
}



fn scheduler_thread_function(dispatcher : Arc<JobDispatcher>, thread_idx : i32) {
    println!("Spawn Thread {}", thread_idx);

    
    println!("Kill Thread {}", thread_idx);
}