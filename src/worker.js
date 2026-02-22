import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars.");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

const WORKER_ID = `worker-${Math.random().toString(16).slice(2)}`;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getNextQueuedJob() {
  const { data, error } = await supabase
    .from("scc_crawl_jobs")
    .select("*")
    .eq("status", "queued")
    .order("created_at", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function run() {
  console.log("MQ SEO Worker started:", WORKER_ID);

  while (true) {
    try {
      const job = await getNextQueuedJob();

      if (!job) {
        await sleep(2000);
        continue;
      }

      console.log("Picked job:", job.id, "snapshot:", job.snapshot_id);

      // Start job (sets snapshot to discovering)
      await supabase.rpc("scc_start_crawl_job", { p_job_id: job.id });

      // Heartbeat proof
      await supabase.rpc("scc_job_heartbeat", { p_job_id: job.id });

      // For now, complete immediately (we’ll replace with real crawl loop next)
      await supabase.rpc("scc_complete_crawl_job", {
        p_job_id: job.id,
        p_success: true
      });

      console.log("Completed job:", job.id);
    } catch (e) {
      console.error("Worker loop error:", e?.message || e);
      await sleep(3000);
    }
  }
}

run();
