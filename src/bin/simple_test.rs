#![feature(drain_filter)]
#[allow(non_snake_case)]

extern crate anarchy_jobsystem;

use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize,Ordering};
use std::thread;

use anarchy_jobsystem::scheduler;

fn main() {

    let mut sched = scheduler::initialize_scheduler(4);

    let callId = Arc::new(AtomicIsize::new(0));

    let job_start = Arc::new(scheduler::JobFence::new(true));

    let jobA = sched.schedule_job(& [job_start], "Job A", scheduler::test_job_function_expected_call_id(callId.clone(), 0));
    let jobB = sched.schedule_job(& [jobA.clone()], "Job B", scheduler::test_job_function_expected_call_id_range(callId.clone(), 1, 3));
    let jobC = sched.schedule_job(& [jobA.clone()], "Job C", scheduler::test_job_function_expected_call_id_range(callId.clone(), 1, 3));
    let jobD = sched.schedule_job(& [jobB.clone(), jobC.clone()], "Job D", scheduler::test_job_function_expected_call_id(callId.clone(), 3));
    
    assert!(jobD.wait(Duration::from_secs(1)));
    assert!(jobD.is_signaled());
    assert_eq!(4, callId.load(Ordering::Relaxed));

    scheduler::shutdown_scheduler(sched);
    
} 