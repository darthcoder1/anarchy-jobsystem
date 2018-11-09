extern crate anarchy_jobsystem;

use anarchy_jobsystem::scheduler;

fn main() {
    
    let job_sched = scheduler::initialize_scheduler(4);

    scheduler::shutdown_scheduler(job_sched);
    
    println!("Hello World."); 
    
}