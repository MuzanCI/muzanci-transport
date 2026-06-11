pub type WorkerId = u64;

pub type RunnerId = u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunnerConfig {
    /// The runner identifier.
    runner_id: RunnerId,

    /// The worker capacity to consume for this runner.
    worker_capacity: u64,

    /// GitHub App installation access token.
    /// Used for fetching code contents and updating checks.
    access_token: String,

    /// The GitHub repository owner.
    repo_owner: String,

    /// The GitHub repository name.
    repo_name: String,

    /// The specific commit to checkout and run the job on.
    commit_sha: String,

    /// The command to execute from the repository root directory.
    exec: String,
}

impl RunnerConfig {
    pub fn new(
        runner_id: RunnerId,
        worker_capacity: u64,
        access_token: String,
        repo_owner: String,
        repo_name: String,
        commit_sha: String,
        exec: String,
    ) -> Self {
        RunnerConfig {
            runner_id,
            worker_capacity,
            access_token,
            repo_owner,
            repo_name,
            commit_sha,
            exec,
        }
    }

    pub fn runner_id(&self) -> RunnerId {
        self.runner_id
    }

    pub fn worker_capacity(&self) -> u64 {
        self.worker_capacity
    }

    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    pub fn repo_owner(&self) -> &str {
        &self.repo_owner
    }

    pub fn repo_name(&self) -> &str {
        &self.repo_name
    }

    pub fn commit_sha(&self) -> &str {
        &self.commit_sha
    }

    pub fn exec(&self) -> &str {
        &self.exec
    }
}
