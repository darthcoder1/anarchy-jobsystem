use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool,Ordering};

pub struct JobFence {
    signaled : AtomicBool,
}

impl JobFence {
    pub fn new(signaled : bool) -> JobFence {
        JobFence { signaled : AtomicBool::new(signaled) }
    }

    pub fn wait(&self, timeout : Duration) {
        while self.signaled.load(Ordering::Relaxed) {
            thread::sleep(timeout);
        }
    }
}

pub struct JobScheduler{
    // JoinHandles for all running job threads
    threads : Vec<thread::JoinHandle<()>>,
    // dispatcher object to allow job threads to get access to the
    // pending_jobs
    controller : Arc<JobController>
}

struct JobInfo {
    // human readable name of the job
    name : String,
    // Vector of dependent job fences, aka this Job can
    // not run before all dependecies have run
    dependencies : Vec<Arc<JobFence>>,
    // job function to execute
    func : Box<Fn() + Send + Sync + 'static>,
    // fence to trigger when the job is finished
    out_fence : Arc<JobFence>,
}

impl JobInfo {
    pub fn is_ready_to_run(&self) -> bool {
        (self.dependencies.len() == 0 || self.dependencies.iter().any(|item| item.signaled.load(Ordering::Relaxed)))
    }
}

struct JobController {
    // Vector of jobs that still need to be executed
    pending_jobs : Mutex<Vec<JobInfo>>,

    // Vector of ready to run jobs (jobs with all dependencies signaled)
    ready_to_run : Mutex<Vec<JobInfo>>,

    // Indicates if the job threads should exit
    exit_threads : AtomicBool,
}

enum JobControllerStatus {
    ExecuteJob(JobInfo),
    WaitForJob,
    ExitRequested,
}

impl JobController {

    fn new() -> JobController {
        JobController {
            pending_jobs : Mutex::new(vec![]),
            ready_to_run: Mutex::new(vec![]),
            exit_threads: AtomicBool::new(false),
        }
    }

    fn update_ready_to_run_jobs(&self) {
        let mut all_jobs = self.pending_jobs.lock().unwrap();
        let mut ready_jobs = all_jobs.drain_filter(|x| x.is_ready_to_run() ).collect::<Vec<_>>();

        let mut ready_job_list = self.ready_to_run.lock().unwrap();
        ready_job_list.append(& mut ready_jobs);
    }

    fn get_next_action(&self) -> JobControllerStatus {

        let mut job_list = self.ready_to_run.lock().unwrap();
        
        if self.exit_threads.load(Ordering::Relaxed) {
            JobControllerStatus::ExitRequested
        }
        else if job_list.len() > 0 {
            JobControllerStatus::ExecuteJob(job_list.pop().unwrap())
        } else {
            JobControllerStatus::WaitForJob
        }
    }
}

pub fn initialize_scheduler(num_job_threads : i32) -> JobScheduler {

    let mut sched = JobScheduler {
        threads : vec![],
        controller: Arc::new(JobController::new()),
    };

    for i in 0..num_job_threads {
        let mut controller = sched.controller.clone();
        sched.threads.push(thread::spawn(move || { scheduler_thread_function(& controller, i); } ));
    }

    sched
}

pub fn shutdown_scheduler(sched : JobScheduler) {

    sched.controller.exit_threads.store(true, Ordering::Relaxed);

    for thread in sched.threads {
        thread.join().unwrap();
    }
}

impl JobScheduler {

    pub fn schedule_job(& mut self, dependencies : & [Arc<JobFence>], job_func : Box<Fn() + Send + Sync + 'static>, job_name : &str) -> Arc<JobFence> {
        let job_info = JobInfo {
            name : String::from(job_name),
            dependencies: dependencies.to_vec(),
            func: job_func,
            out_fence: Arc::new(JobFence { signaled: AtomicBool::from(false) }),
        };

        let out_fence = job_info.out_fence.clone();

        let mut was_empty = false;
        {
            let mut pending_jobs = self.controller.pending_jobs.lock().unwrap();
            was_empty = pending_jobs.len() == 0;
            pending_jobs.push(job_info);
        }

        if was_empty {
            self.controller.update_ready_to_run_jobs();
        }

        out_fence
    }
}

fn scheduler_thread_function(controller : & JobController, thread_idx : i32) {

    println!("Spawn Thread {}", thread_idx);

    loop {

        let next_action = controller.get_next_action();

        match next_action {
            JobControllerStatus::ExecuteJob(job) => {
                println!("Execute job {}", job.name);
                (job.func)();
                job.out_fence.signaled.store(true, Ordering::Relaxed);

                controller.update_ready_to_run_jobs();
            },
            JobControllerStatus::WaitForJob => thread::sleep(Duration::from_millis(0)),
            JobControllerStatus::ExitRequested => break,
        }
    }

    println!("Kill Thread {}", thread_idx);
}