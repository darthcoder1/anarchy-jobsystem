extern crate anarchy_jobsystem;


pub struct Foo {
    i : i32,
}

fn main() {
    let a = Foo { i : 10 };

    println!("Hello World. {}", a.i);
    
}