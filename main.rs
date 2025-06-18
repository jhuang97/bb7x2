use std::{fs, io::{BufRead, BufReader}, path::Path, process::{Child, Command, Stdio}, thread, time::{self, Duration}};
use std::sync::{Arc, Mutex};
use circular_buffer::CircularBuffer;
use crossbeam_queue::SegQueue;
use std::time::Instant;
use pretty_duration::{pretty_duration, PrettyDurationOptions, PrettyDurationOutputFormat};

const WORKING_DIR: &'static str = "/path/to/some/folder";
const COMPLETED_DIR: &'static str = "/some/other/folder";
const REPORT_INTERVAL: Duration = time::Duration::from_secs(10);
const N_WORKERS: usize = 5; // how many cores you want to use for this

const MXDYS_BB7_TM_EXECUTABLE: &'static str = "/path/to/mxdys/executable";
const PYTHON_EXECUTABLE: &'static str = "python";
const PYENV_VIRTUALENV: Option<&'static str> = None; // likely not relevant unless you have a pyenv virtual environment like I do
const ENUMERATE_PY_PATH: &'static str = "/something/something/Code/Enumerate.py";

fn main() {
    let task_ids = SegQueue::new();
    for i in 0..10 {
        task_ids.push(i);
    }

    let start_time = Instant::now();
    let worker_group = WorkerGroup::new(N_WORKERS, task_ids);

    loop {
        if worker_group.is_done() {
            worker_group.join_all();
            let duration_s = pretty_duration(&start_time.elapsed(), Some(COMPACT_OPTIONS));
            println!("All done, {duration_s}.");
            io::stdout().flush().expect("failed to flush stdout");
            break;
        }
        worker_group.print_status();
        thread::sleep(REPORT_INTERVAL);
    }
}

const COMPACT_OPTIONS: PrettyDurationOptions = PrettyDurationOptions {
    output_format: Some(PrettyDurationOutputFormat::Compact),
    singular_labels: None,
    plural_labels: None,
};

struct Worker {
    id: usize,
    thread: thread::JoinHandle<()>,
    task_info: Arc<Mutex<TaskInfo>>,
}

impl Worker {
    fn new(id: usize, job_queue: Arc<SegQueue<u32>>) -> Worker {
        let task_info = Arc::new(Mutex::new(TaskInfo::new()));
        let info1 = task_info.clone();

        let thread = thread::spawn(move || loop {
            match job_queue.pop() {
                Some(task_id) => {
                    combined_enumeration(task_id, Arc::clone(&info1));
                }
                None => {
                    let mut info_inner = info1.lock().unwrap();
                    println!("Worker {id} finished. {}", info_inner.out_buf.back().unwrap_or(&"buffer empty".to_owned()));
                    info_inner.kind = TaskKind::Done;
                    break;
                }
            }
        });
        Worker { id, thread, task_info }
    }
}

struct WorkerGroup {
    workers: Vec<Worker>,
    job_queue: Arc<SegQueue<u32>>,
}

impl WorkerGroup {
    fn new(size: usize, task_ids: SegQueue<u32>) -> WorkerGroup {
        let job_queue = Arc::new(task_ids);
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&job_queue)));
        }
        WorkerGroup { workers, job_queue }
    }

    fn _clear_pending_jobs(&mut self) {
        while !self.job_queue.is_empty() {
            self.job_queue.pop();
        }
    }

    fn print_status(&self) {
        let mut status_start = ">"; // visually indicate the first line of the status report
        for w in &self.workers {
            let info2 = w.task_info.lock().unwrap();
            if info2.kind != TaskKind::Done {
                let elapsed = info2.start_time.elapsed();
                println!("{status_start}[{}] {:?} - elapsed {:.2?}; last line: {}", w.id, info2.kind, elapsed, 
                    info2.out_buf.back().unwrap_or(&"buffer empty".to_owned()));
                status_start = " ";
            }
        }
    }

    fn is_done(&self) -> bool {
        for w in &self.workers {
            let info2 = w.task_info.lock().unwrap();
            if info2.kind != TaskKind::Done {
                return false;
            }
        }
        true
    }

    /// joins all workers, one at a time. Probably should not call this unless all workers are confirmed to be finished by other means
    fn join_all(self) {
        for w in self.workers {
            w.thread.join().expect("couldn't join thread");
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum TaskKind {
    Uninit,
    MxdysEnum(u32),
    SligockiEnum(u32),
    MoveFile,
    Done
}

pub struct TaskInfo {
    kind: TaskKind,
    start_time: Instant,
    out_buf: CircularBuffer<20, String>,
    err_buf: Vec<String>,
}

impl TaskInfo {
    fn new() -> Self {
        Self {
            kind: TaskKind::Uninit,
            start_time: Instant::now(),
            out_buf: CircularBuffer::new(),
            err_buf: Vec::new(),
        }
    }
}

pub struct ProcessWithBuffer {
    child: Child,
    info: Arc<Mutex<TaskInfo>>,
}

impl ProcessWithBuffer {
    pub fn new(command: &mut Command, info: Arc<Mutex<TaskInfo>>) -> std::io::Result<Self>  {
        {
            let mut info_guard = info.lock().unwrap();
            info_guard.start_time = Instant::now();
        }

        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = child
            .stdout
            .take()
            .expect("Accessing stdout should never fail after passing Stdio::piped().");

        let stderr = child
            .stderr
            .take()
            .expect("Accessing stdout should never fail after passing Stdio::piped().");

        let info1 = info.clone();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let mut info = info1.lock().unwrap();
                match line {
                    Ok(s) => info.out_buf.push_back(s),
                    Err(e) => info.out_buf.push_back(e.to_string()),
                };
            }
        });

        let info2 = info.clone();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let mut info = info2.lock().unwrap();
                match line {
                    Ok(s) => info.err_buf.push(s),
                    Err(e) => info.err_buf.push(e.to_string()),
                };
            }
        });

        Ok(ProcessWithBuffer { child, info })
    }
}

fn make_sligocki_command(task_id: u32) -> Command {
    let mut sligocki_enum = Command::new(PYTHON_EXECUTABLE);
    if let Some(venv_name) = PYENV_VIRTUALENV {
        sligocki_enum.env("PYENV_VERSION", venv_name);
    }
    sligocki_enum.args([ENUMERATE_PY_PATH,
            "--infile",
            &format!("holdouts_{}.txt", task_id),
            "--outfile",
            &format!("bb7_{:06}.out.pb", task_id),
            "-r",
            "--no-steps",
            "--exp-linear-rules",
            "--max-loops=100_000",
            "--block-mult=2",
            "--time=30",
            "--force",
            "--save-freq=50",
            "--debug-print-current"
        ])
        .current_dir(WORKING_DIR);
    sligocki_enum
}

fn start_sligocki_task(task_id: u32, info: Arc<Mutex<TaskInfo>>) -> ProcessWithBuffer {
    let mut command = make_sligocki_command(task_id);
    {
        let mut info2 = info.lock().unwrap();
        info2.kind = TaskKind::SligockiEnum(task_id);
        info2.err_buf.clear();
    }
    ProcessWithBuffer::new(&mut command, info).expect("failed to start job")
}

fn start_mxdys_task(task_id: u32, info: Arc<Mutex<TaskInfo>>) -> ProcessWithBuffer {
    let mut command = Command::new(MXDYS_BB7_TM_EXECUTABLE);
    command.args(["enum", &format!("{}", task_id)])
        .current_dir(WORKING_DIR);
    {
        let mut info2 = info.lock().unwrap();
        info2.kind = TaskKind::MxdysEnum(task_id);
        info2.err_buf.clear();
    }
    ProcessWithBuffer::new(&mut command, info).expect("failed to start job")
}

fn combined_enumeration(task_id: u32, info: Arc<Mutex<TaskInfo>>) {
    {
        let mut mxdys_job = start_mxdys_task(task_id, info.clone());
        let exit_str = mxdys_job.child.wait().map_or("already exited".to_owned(), 
            |r| if r.success() {
                "success".to_owned()
            } else {
                format!("{}", r)
            });
        let elapsed = {
            let info2 = mxdys_job.info.lock().unwrap();
            info2.start_time.elapsed()
        };
        println!("mxdys enum {task_id} finished, {:.2?}, {exit_str}", elapsed);
    }

    {
        let mut sligocki_job = start_sligocki_task(task_id, info.clone());
        let exit_str = sligocki_job.child.wait().map_or("already exited".to_owned(), 
            |r| if r.success() {
                "success".to_owned()
            } else {
                format!("{}", r)
            });
        let elapsed = {
            let info2 = sligocki_job.info.lock().unwrap();
            info2.start_time.elapsed()
        };
        println!("sligocki enum {task_id} finished, {:.2?}, {exit_str}", elapsed);
    }

    {
        let mut info2 = info.lock().unwrap();
        info2.kind = TaskKind::MoveFile;
    }
    let fname = format!("bb7_{:06}.out.pb", task_id);
    let path_from = Path::new(WORKING_DIR).join(&fname);
    let path_to = Path::new(COMPLETED_DIR).join(&fname);
    if path_from.exists() {
        match fs::rename(&path_from, &path_to) {
            Err(_) => eprintln!("failed to move file: {path_from:?} to {path_to:?}"),
            _ => (),
        }
    } else {
        eprintln!("Could not find {path_from:?}");
    }
}
