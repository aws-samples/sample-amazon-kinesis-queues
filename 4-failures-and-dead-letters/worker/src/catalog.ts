import { GetParameterCommand, SSMClient } from '@aws-sdk/client-ssm';

/** How long a read of a parameter is reused before asking SSM again. */
const CACHE_MS = 10_000;

/** An SSM parameter read at run time and cached for CACHE_MS. */
class CachedParameter<T> {
  private cached?: { value: T; at: number };

  constructor(private readonly ssm: SSMClient, private readonly name: string, private readonly parse: (raw: string) => T) {}

  async get(): Promise<T> {
    if (this.cached && Date.now() - this.cached.at < CACHE_MS) return this.cached.value;
    const { Parameter } = await this.ssm.send(new GetParameterCommand({ Name: this.name }));
    const value = this.parse(Parameter?.Value?.trim() ?? '');
    this.cached = { value, at: Date.now() };
    return value;
  }
}

/**
 * Operator-controlled settings, read from SSM parameters at run time.
 *
 * - The product catalog's discontinued SKUs: a comma-separated list, or "none".
 *   An operator changes it (npm run orders:catalog) to fix the cause of failing orders.
 * - The simulated warehouse's status: "up" or "down" (npm run orders:warehouse), to
 *   simulate an outage of the downstream API.
 *
 * Running workers pick a change up within CACHE_MS, so neither needs a redeploy or a
 * restart: the workers keep running unchanged.
 */
export class Catalog {
  private readonly ssm: SSMClient;
  private readonly skus: CachedParameter<Set<string>>;
  private readonly warehouse: CachedParameter<boolean>;

  constructor(region: string, catalogParameter: string, warehouseParameter: string) {
    this.ssm = new SSMClient({ region });
    this.skus = new CachedParameter(this.ssm, catalogParameter, (value) => new Set(
      value.toLowerCase() === 'none' ? [] : value.split(',').map((s) => s.trim()).filter(Boolean),
    ));
    this.warehouse = new CachedParameter(this.ssm, warehouseParameter, (value) => value.toLowerCase() !== 'down');
  }

  discontinuedSkus(): Promise<Set<string>> {
    return this.skus.get();
  }

  warehouseUp(): Promise<boolean> {
    return this.warehouse.get();
  }
}
