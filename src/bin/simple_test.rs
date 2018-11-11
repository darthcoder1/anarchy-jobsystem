#![feature(drain_filter)]

extern crate anarchy_jobsystem;

use std::time::Duration;
use std::sync::Arc;
use std::thread;

use anarchy_jobsystem::scheduler;

fn main() {
    
    let mut job_sched = scheduler::initialize_scheduler(4);

    let fence = Arc::new(scheduler::JobFence::new(true));

    let fence1 = job_sched.schedule_job(& [fence.clone()], Box::new(|| { println!("Execute JOB 0");}), "Job-A");
    let fence2 = job_sched.schedule_job(& [fence1.clone()], Box::new(|| {println!("Execute JOB 1 after JOB 0" )}), "Job-B");
    let fence3 = job_sched.schedule_job(& [fence2.clone()], Box::new(|| {println!("Execute JOB 2 after JOB 1" )}), "Job-C");
    let fence4 = job_sched.schedule_job(& [fence.clone()], Box::new(|| {println!("Execute JOB 3" )}), "Job-C");

    fence4.wait(Duration::from_millis(0));
    thread::sleep(Duration::from_millis(1000));

    scheduler::shutdown_scheduler(job_sched);
    
    println!("Hello World."); 
    
}