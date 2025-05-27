import { Queue } from 'bull';
import { Gauge, Summary } from 'prom-client';

export interface BullMetricsOptions {
  promClient: any;
  interval?: number;
  useGlobal?: boolean;
}

export interface BullMetricsInstance {
  stop: () => void;
  remove: () => void;
}

const LABEL_NAMES = {
  QUEUE_PREFIX: 'queue_prefix',
  QUEUE_NAME: 'queue_name',
  JOB_NAME: 'job_name',
  STATUS: 'status',
  ERROR_TYPE: 'error_type',
};

const STATUS_TYPES = {
  COMPLETED: 'completed',
  FAILED: 'failed',
};

export function init(options: BullMetricsOptions) {
  const interval = options.interval || 60000;
  const useGlobal = options.useGlobal || false;
  const promClient = options.promClient;

  const jobs_completed_total = new promClient.Gauge({
    name: 'jobs_completed_total',
    help: 'Total completed jobs',
    labelNames: [LABEL_NAMES.QUEUE_PREFIX, LABEL_NAMES.QUEUE_NAME],
  });

  const jobs_active_total = new promClient.Gauge({
    name: 'jobs_active_total',
    help: 'Total active jobs',
    labelNames: [LABEL_NAMES.QUEUE_PREFIX, LABEL_NAMES.QUEUE_NAME],
  });

  const jobs_delayed_total = new promClient.Gauge({
    name: 'jobs_delayed_total',
    help: 'Total delayed jobs',
    labelNames: [LABEL_NAMES.QUEUE_PREFIX, LABEL_NAMES.QUEUE_NAME],
  });

  const jobs_failed_total = new promClient.Gauge({
    name: 'jobs_failed_total',
    help: 'Total failed jobs',
    labelNames: [LABEL_NAMES.QUEUE_PREFIX, LABEL_NAMES.QUEUE_NAME],
  });

  const jobs_waiting_total = new promClient.Gauge({
    name: 'jobs_waiting_total',
    help: 'Total waiting jobs',
    labelNames: [LABEL_NAMES.QUEUE_PREFIX, LABEL_NAMES.QUEUE_NAME],
  });

  const job_duration = new promClient.Summary({
    name: 'job_duration',
    help: 'Job duration',
    labelNames: [
      LABEL_NAMES.QUEUE_PREFIX,
      LABEL_NAMES.QUEUE_NAME,
      LABEL_NAMES.JOB_NAME,
      LABEL_NAMES.STATUS,
      LABEL_NAMES.ERROR_TYPE,
    ],
  });

  const job_wait_duration = new promClient.Summary({
    name: 'job_wait_duration',
    help: 'Job waiting duration',
    labelNames: [
      LABEL_NAMES.QUEUE_PREFIX,
      LABEL_NAMES.QUEUE_NAME,
      LABEL_NAMES.JOB_NAME,
      LABEL_NAMES.STATUS,
      LABEL_NAMES.ERROR_TYPE,
    ],
  });

  const job_attempts = new promClient.Summary({
    name: 'job_attempts',
    help: 'Job attempts',
    labelNames: [
      LABEL_NAMES.QUEUE_PREFIX,
      LABEL_NAMES.QUEUE_NAME,
      LABEL_NAMES.JOB_NAME,
      LABEL_NAMES.STATUS,
      LABEL_NAMES.ERROR_TYPE,
    ],
  });

  function recordJobMetrics(labels: Record<string, string>, status: string, job: any) {
    try {
      if (!job) {
        console.error('No job data provided to recordJobMetrics');
        return;
      }

      const now = Date.now();
      const duration = job.finishedOn ? job.finishedOn - job.processedOn : now - (job.processedOn || now);
      const waitDuration = job.processedOn ? job.processedOn - job.timestamp : now - job.timestamp;

      console.debug('Recording metrics for job:', {
        id: job.id,
        name: job.name,
        duration,
        waitDuration,
        attempts: job.attemptsMade,
        timestamps: {
          created: job.timestamp,
          processed: job.processedOn,
          finished: job.finishedOn
        }
      });

      job_duration.observe(duration, { ...labels, status });
      job_wait_duration.observe(waitDuration, { ...labels, status });
      job_attempts.observe(job.attemptsMade || 0, { ...labels, status });
    } catch (err) {
      console.error('Error recording job metrics:', err, 'Job:', job);
    }
  }

  return {
    start(queue: Queue): BullMetricsInstance {
      const queueName = queue.name;
      const prefix = queue.client.options.keyPrefix.replace(':', '') || 'bull';
      const labels = {
        [LABEL_NAMES.QUEUE_PREFIX]: prefix,
        [LABEL_NAMES.QUEUE_NAME]: queueName,
      };

      const completedEvent = useGlobal ? 'global:completed' : 'completed';
      const failedEvent = useGlobal ? 'global:failed' : 'failed';

      queue.on(completedEvent, (job) => {
        try {
          console.debug('Completed event received for job:', { id: job.id, name: job.name });
          const jobLabels = {
            ...labels,
            [LABEL_NAMES.JOB_NAME]: job.name || 'default',
            [LABEL_NAMES.STATUS]: STATUS_TYPES.COMPLETED,
            [LABEL_NAMES.ERROR_TYPE]: '',
          };
          recordJobMetrics(jobLabels, STATUS_TYPES.COMPLETED, job);
        } catch (err) {
          console.error('Error handling completed event:', err, 'Job:', job);
        }
      });

      queue.on(failedEvent, (job, err) => {
        try {
          console.debug('Failed event received for job:', { id: job.id, name: job.name, error: err?.message });
          const jobLabels = {
            ...labels,
            [LABEL_NAMES.JOB_NAME]: job.name || 'default',
            [LABEL_NAMES.STATUS]: STATUS_TYPES.FAILED,
            [LABEL_NAMES.ERROR_TYPE]: err?.name || 'Error',
          };
          recordJobMetrics(jobLabels, STATUS_TYPES.FAILED, job);
        } catch (err) {
          console.error('Error handling failed event:', err, 'Job:', job);
        }
      });

      const metricsInterval = setInterval(async () => {
        try {
          const counts = await queue.getJobCounts();
          const jobs = {
            completed: await queue.getJobs(['completed'], 0, 100),
            active: await queue.getJobs(['active'], 0, 100),
            delayed: await queue.getJobs(['delayed'], 0, 100),
            failed: await queue.getJobs(['failed'], 0, 100),
            waiting: await queue.getJobs(['waiting'], 0, 100),
          };

          // Group jobs by name
          const jobsByName = {
            completed: groupJobsByName(jobs.completed),
            active: groupJobsByName(jobs.active),
            delayed: groupJobsByName(jobs.delayed),
            failed: groupJobsByName(jobs.failed),
            waiting: groupJobsByName(jobs.waiting),
          };

          // Update metrics for each job name
          Object.entries(jobsByName.completed).forEach(([jobName, count]) => {
            jobs_completed_total.set({ ...labels, [LABEL_NAMES.JOB_NAME]: jobName }, count);
          });
          Object.entries(jobsByName.active).forEach(([jobName, count]) => {
            jobs_active_total.set({ ...labels, [LABEL_NAMES.JOB_NAME]: jobName }, count);
          });
          Object.entries(jobsByName.delayed).forEach(([jobName, count]) => {
            jobs_delayed_total.set({ ...labels, [LABEL_NAMES.JOB_NAME]: jobName }, count);
          });
          Object.entries(jobsByName.failed).forEach(([jobName, count]) => {
            jobs_failed_total.set({ ...labels, [LABEL_NAMES.JOB_NAME]: jobName }, count);
          });
          Object.entries(jobsByName.waiting).forEach(([jobName, count]) => {
            jobs_waiting_total.set({ ...labels, [LABEL_NAMES.JOB_NAME]: jobName }, count);
          });
        } catch (err) {
          console.error('Error fetching job counts:', err);
        }
      }, interval);

      function groupJobsByName(jobs: any[]) {
        return jobs.reduce((acc, job) => {
          const name = job.name || 'default';
          acc[name] = (acc[name] || 0) + 1;
          return acc;
        }, {});
      }

      return {
        stop() {
          clearInterval(metricsInterval);
        },
        remove() {
          clearInterval(metricsInterval);
          jobs_completed_total.remove(labels);
          jobs_active_total.remove(labels);
          jobs_delayed_total.remove(labels);
          jobs_failed_total.remove(labels);
          jobs_waiting_total.remove(labels);
          job_duration.remove(labels);
          job_wait_duration.remove(labels);
          job_attempts.remove(labels);
        },
      };
    },
  };
} 