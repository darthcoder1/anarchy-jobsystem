
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;

pub struct JobFence {
    signaled : AtomicBool,
}

pub struct JobScheduler{
    // JoinHandles for all running job threads
    threads : Vec<thread::JoinHandle<()>>,
    // Vector of jobs that still need to be executed
    pending_jobs : Mutex<Vec<JobInfo>>,
    // dispatcher object to allow job threads to get access to the
    // pending_jobs
    dispatcher : Arc<JobDispatcher>
}

struct JobInfo {
    // Vector of dependent job fences, aka this Job can
    // not run before all dependecies have run
    dependencies : Vec<Arc<JobFence>>,
    // job function to execute
    func : Box<Fn() + Send + Sync + 'static>,
    // fence to trigger when the job is finished
    out_fence : Arc<JobFence>,
}

struct JobDispatcher {
    // Vector of ready to run jobs (jobs with all dependencies sigaled)
    ready_to_run : Mutex<Vec<JobInfo>>,
}

impl JobDispatcher {

    fn new() -> JobDispatcher {
        JobDispatcher {
            ready_to_run: Mutex::new(vec![]),
        }
    }

    fn push_job_to_run(&self, job : JobInfo) {
        let mut job_list = self.ready_to_run.lock().unwrap();

        job_list.push(job);
    }

    fn pull_job_to_run(&self) -> Result<JobInfo, ()> {
        let mut job_list = self.ready_to_run.lock().unwrap();
        
        if job_list.len() > 0 {
            Ok(job_list.pop().unwrap())
        } else {
            Err(())
        }
    }
}

pub fn initialize_scheduler(num_job_threads : i32) -> JobScheduler {

    let mut sched = JobScheduler {
        threads : vec![],
        pending_jobs : Mutex::new(vec![]),
        dispatcher: Arc::new(JobDispatcher::new()),
    };

    for i in 0..num_job_threads {
        let mut dispatcher = sched.dispatcher.clone();
        sched.threads.push(thread::spawn(move || { scheduler_thread_function(& dispatcher, i); } ));
    }

    sched
}

pub fn shutdown_scheduler(sched : JobScheduler) {

    for thread in sched.threads {
        thread.join().unwrap();
    }
}

impl JobScheduler {

    pub fn schedule_job(& mut self, dependencies : & [Arc<JobFence>], job_func : Box<Fn() + Send + Sync + 'static>) -> Arc<JobFence> {
        let job_info = JobInfo {
            dependencies: dependencies.to_vec(),
            func: job_func,
            out_fence: Arc::new(JobFence { signaled: AtomicBool::from(false) }),
        };

        let out_fence = job_info.out_fence.clone();

        self.pending_jobs.lock().unwrap()
            .push(job_info);
        
        out_fence
    }
}

fn scheduler_thread_function(dispatcher : & JobDispatcher, thread_idx : i32) {

    println!("Spawn Thread {}", thread_idx);

    loop {

        let job_to_run = dispatcher.pull_job_to_run();

        match job_to_run {
            Ok(job) => (job.func)(),
            _ => (),
        }
    }
}