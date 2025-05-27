import { Queue } from 'bull';
import { createBullBoard } from '@bull-board/api';
import { BullAdapter } from '@bull-board/api/bullAdapter';
import { ConfigService } from '../../config/config.service';
import { LoggerService } from '../../logger';
import { IBullUi } from '../bull.interfaces';
import { ExpressAdapter } from '@bull-board/express';
/**
 * On the hacky side. This has the internal information for how
 * queues are stored in bull board. Since we are adding/removing
 * periodically we want to be able to modify these values in response
 * to incoming redis events. There are currently existing methods
 * setQueues and replaceQueues but they seem a bit heavy handed
 */
interface BullBoardLocals {
  bullBoardQueues: Map<string, BullAdapter>;
}

export class BullBoardUi implements IBullUi {
  private readonly _ui: ExpressAdapter; //ReturnType<typeof createBullBoard>;
  private readonly _board: ReturnType<typeof createBullBoard>;

  constructor(
    private readonly logger: LoggerService,
    private readonly configService: ConfigService,
  ) {
    this._ui = new ExpressAdapter();
    this._ui.setBasePath('/queues');

    this._board = createBullBoard({ queues: [], serverAdapter: this._ui });
  }

  addQueue(queuePrefix: string, queueName: string, queue: Queue) {
    const queueKey = `${queuePrefix}:${queueName}`;
    this._board.addQueue(new BullAdapter(queue))
    // (this._ui.router.locals as BullBoardLocals).bullBoardQueues.set(
    //   queueKey,
    //   new BullAdapter(queue),
    // );
  }

  removeQueue(queuePrefix: string, queueName: string) {
    this._board.removeQueue(queueName);
  }

  get middleware() {
    return this._ui.getRouter();
  }
}
