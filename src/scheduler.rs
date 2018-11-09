
use std::thread;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub struct JobFence {
    signaled : AtomicBool,
}

pub struct JobScheduler{
    threads : Vec<thread::JoinHandle<()>>,
    pending_jobs : Vec<JobInfo>
}

struct JobInfo {
    dependencies : Vec<Arc<JobFence>>,
    func : Box<Fn()>,
    out_fence : Arc<JobFence>,
}

pub fn initialize_scheduler(num_job_threads : i32) -> JobScheduler {

    let mut sched = JobScheduler {
        threads : vec![],
        pending_jobs : vec![]
    };

    for i in 0..num_job_threads {
        sched.threads.push(thread::spawn(move || { scheduler_thread_function(i); } ));
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



fn scheduler_thread_function(thread_idx : i32) {
    println!("Spawn Thread {}", thread_idx);

    println!("Kill Thread {}", thread_idx);
}