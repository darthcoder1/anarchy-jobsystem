use std::thread;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool,AtomicIsize,Ordering};

pub struct JobFence {
    signaled : AtomicBool,
}

impl JobFence {
    pub fn new(signaled : bool) -> JobFence {
        JobFence { signaled : AtomicBool::new(signaled) }
    }

    pub fn is_signaled(&self) -> bool {
        self.signaled.load(Ordering::Relaxed)
    }

    pub fn signal(&self) {
        assert!(!self.is_signaled());
        self.signaled.store(true, Ordering::Relaxed);
    }

    // Waits for the fence to become signaled or until the timeout.
    // Returns either true (fence got signaled) or false (timeout)
    pub fn wait(&self, timeout : Duration) -> bool{
        let iter_timeout = Duration::from_millis(100);
        let mut time_left = timeout;
        
        while !self.is_signaled() && time_left > Duration::new(0,0) {
            thread::sleep(iter_timeout);
            time_left -= iter_timeout;
        }

        (time_left > Duration::new(0,0))
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
        (self.dependencies.len() == 0 || self.dependencies.iter().all(|item| item.signaled.load(Ordering::Relaxed)))
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

    pub fn schedule_job(& mut self, 
                        dependencies : & [Arc<JobFence>], 
                        job_name : &str, 
                        job_func : Box<Fn() + Send + Sync + 'static>) -> Arc<JobFence> 
    {
        
        let job_info = JobInfo {
            name : String::from(job_name),
            dependencies: dependencies.to_vec(),
            func: job_func,
            out_fence: Arc::new(JobFence { signaled: AtomicBool::from(false) }),
        };

        let out_fence = job_info.out_fence.clone();

        let mut  was_empty = false;
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

    loop {
        let next_action = controller.get_next_action();

        match next_action {
            JobControllerStatus::ExecuteJob(job) => {
                (job.func)();
                job.out_fence.signal();
                controller.update_ready_to_run_jobs();
            },
            JobControllerStatus::WaitForJob => {
                thread::sleep(Duration::from_millis(0));
                // TODO: fall asleep until jobs are in queue
                controller.update_ready_to_run_jobs();
            },
            JobControllerStatus::ExitRequested => break,
        }
    }
}

// returns a function for a job, that increments the atomic counter after it checked whether the current value of the counter
// is equal to expected_call_idx
pub fn test_job_function_expected_call_id(callId_counter : Arc<AtomicIsize>, expected_call_idx : isize) -> Box<Fn() + Send + Sync + 'static>
{
    let call_id = callId_counter.clone();

    Box::new (move || { 
        let id = call_id.load(Ordering::Relaxed);
        call_id.fetch_add(1, Ordering::Relaxed);

        assert_eq!(expected_call_idx, id);
    })
}

// returns a function for a job, that increments the atomic counter after it checked whether the current value of the counter
// is within the range specified with min_call_idx(inclusive) and max_call_idx(exklusiv)
pub fn test_job_function_expected_call_id_range(callId_counter : Arc<AtomicIsize>, min_call_idx : isize, max_call_idx : isize) 
    -> Box<Fn() + Send + Sync + 'static>
{
    Box::new (move || { 
        let id = callId_counter.load(Ordering::Relaxed);
        
        callId_counter.fetch_add(1, Ordering::Relaxed);

        assert!(id >= min_call_idx && id <  max_call_idx);
    })
}


#[cfg(test)]
mod tests {

    use super::super::scheduler;

    use std::time::Duration;
    use std::sync::atomic::{AtomicIsize,Ordering};
    use std::sync::Arc;

    use scheduler::test_job_function_expected_call_id;
    use scheduler::test_job_function_expected_call_id_range;

    #[test]
    fn JobFence_wait_ReturnsTrueAfterSignaled() {
        let mut fence = scheduler::JobFence::new(false);
        
        assert!(!fence.is_signaled());
        
        fence.signal();
        
        assert!(fence.is_signaled());
        assert!(fence.wait(Duration::from_millis(100)));
    }

    #[test]
    fn JobFence_wait_ReturnsFalseAfterTimeout() {
        let mut fence = scheduler::JobFence::new(false);
        
        assert!(!fence.is_signaled());
        assert!(!fence.wait(Duration::from_millis(100)));
    }

    #[test]
    fn JobScheduler_With4Threads_InitializesThreadsCorrectly(){
    
        let mut sched = scheduler::initialize_scheduler(4);

        assert_eq!(4, sched.threads.len());
        scheduler::shutdown_scheduler(sched);
    }

    #[test]
    fn JobScheduler_Executes1Job(){

        let mut sched = scheduler::initialize_scheduler(4);

        let callId = Arc::new(AtomicIsize::new(0));

        let fence = sched.schedule_job(& [Arc::new(scheduler::JobFence::new(true))], "TestJob 0", test_job_function_expected_call_id(callId.clone(), 0));

        assert!(fence.wait(Duration::from_secs(1)));
        assert!(fence.is_signaled());
        assert_eq!(1, callId.load(Ordering::Relaxed));

        scheduler::shutdown_scheduler(sched);
    }

    #[test]
    fn JobScheduler_ExecutesJob1AfterJob0(){

        let mut sched = scheduler::initialize_scheduler(4);

        let callId = Arc::new(AtomicIsize::new(0));

        let job_start = Arc::new(scheduler::JobFence::new(true));
        let job0 = sched.schedule_job(& [job_start], "Job 0", test_job_function_expected_call_id(callId.clone(), 0));
        let job1 = sched.schedule_job(& [job0.clone()], "Job 1", test_job_function_expected_call_id(callId.clone(), 1));

        assert!(job1.wait(Duration::from_secs(1)));
        assert!(job1.is_signaled());
        assert!(job0.is_signaled());
        assert_eq!(2, callId.load(Ordering::Relaxed));

        scheduler::shutdown_scheduler(sched);
    }

    #[test]
    fn JobScheduler_ExecutesChain_A_BC_D(){

        let mut sched = scheduler::initialize_scheduler(4);

        let callId = Arc::new(AtomicIsize::new(0));

        let job_start = Arc::new(scheduler::JobFence::new(true));

        let jobA = sched.schedule_job(& [job_start], "Job A", scheduler::test_job_function_expected_call_id(callId.clone(), 0));
        let jobB = sched.schedule_job(& [jobA.clone()], "Job B", scheduler::test_job_function_expected_call_id_range(callId.clone(), 1, 3));
        let jobC = sched.schedule_job(& [jobA.clone()], "Job C", scheduler::test_job_function_expected_call_id_range(callId.clone(), 1, 3));
        let jobD = sched.schedule_job(& [jobB.clone(), jobC.clone()], "Job D", scheduler::test_job_function_expected_call_id(callId.clone(), 3));
        
        assert!(jobD.wait(Duration::from_secs(1)));
        assert!(jobD.is_signaled());
        assert!(jobA.is_signaled());
        assert!(jobB.is_signaled());
        assert!(jobC.is_signaled());
        
        assert_eq!(4, callId.load(Ordering::Relaxed));

        scheduler::shutdown_scheduler(sched);
    }
}
