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
      if (!job.processedOn || !job.timestamp) {
        console.error('Job missing required timing data:', job.id);
        return;
      }

      const duration = job.processedOn ? Date.now() - job.processedOn : 0;
      const waitDuration = job.processedOn && job.timestamp ? job.processedOn - job.timestamp : 0;

      job_duration.observe({ ...labels, status }, duration);
      job_wait_duration.observe({ ...labels, status }, waitDuration);
      job_attempts.observe({ ...labels, status }, job.attemptsMade || 0);
    } catch (err) {
      console.error('Error recording job metrics:', err);
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

      queue.on(completedEvent, async (job) => {
        try {
          const completeJob = await queue.getJob(job.id);
          if (!completeJob) {
            console.error('Could not fetch complete job data for:', job.id);
            return;
          }

          const jobLabels = {
            ...labels,
            [LABEL_NAMES.JOB_NAME]: completeJob.name || 'default',
            [LABEL_NAMES.STATUS]: STATUS_TYPES.COMPLETED,
            [LABEL_NAMES.ERROR_TYPE]: '',
          };
          recordJobMetrics(jobLabels, STATUS_TYPES.COMPLETED, completeJob);
        } catch (err) {
          console.error('Error handling completed event:', err);
        }
      });

      queue.on(failedEvent, async (job, err) => {
        try {
          const completeJob = await queue.getJob(job.id);
          if (!completeJob) {
            console.error('Could not fetch complete job data for:', job.id);
            return;
          }

          const jobLabels = {
            ...labels,
            [LABEL_NAMES.JOB_NAME]: completeJob.name || 'default',
            [LABEL_NAMES.STATUS]: STATUS_TYPES.FAILED,
            [LABEL_NAMES.ERROR_TYPE]: err?.name || 'Error',
          };
          recordJobMetrics(jobLabels, STATUS_TYPES.FAILED, completeJob);
        } catch (err) {
          console.error('Error handling failed event:', err);
        }
      });

      const metricsInterval = setInterval(async () => {
        try {
          const counts = await queue.getJobCounts();
          jobs_completed_total.set(labels, counts.completed);
          jobs_active_total.set(labels, counts.active);
          jobs_delayed_total.set(labels, counts.delayed);
          jobs_failed_total.set(labels, counts.failed);
          jobs_waiting_total.set(labels, counts.waiting);
        } catch (err) {
          console.error('Error fetching job counts:', err);
        }
      }, interval);

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