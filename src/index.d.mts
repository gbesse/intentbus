// Purpose: Type the IntentBus evaluator, state transitions and delivery adapters.
export interface Rule { id: string; description: string; version: string; ttlMs: number; all: { fact: string; on: number; off: number }[] }
export interface Observation { id: string; entityId: string; occurredAt: string; state: unknown }
export interface BusinessEvent { schemaVersion: 1; id: string; sequence: number; type: string; subscription: { id: string; version: string }; evaluatorVersion: string; entityId: string; sourceId: string; occurredAt: string; status: 'active' | 'inactive' | 'unknown'; active: boolean; previousStatus: 'active' | 'inactive' | 'unknown' | null; facts: Record<string, number>; expiresAt: string }
export interface SubscriptionState { entityId: string; ruleId: string; sourceId: string; status: 'active' | 'inactive' | 'unknown'; active: boolean; facts: Record<string, number>; expiresAt: string; expired?: boolean }
export type Sink = (event: BusinessEvent, options: { signal: AbortSignal }) => Promise<unknown>;
export interface Bus { ingest(observation: Observation): Promise<{ duplicate: boolean; events: BusinessEvent[] }>; expire(at?: string): Promise<BusinessEvent[]>; pending(): BusinessEvent[]; states(): SubscriptionState[]; flush(sink: Sink): Promise<number>; close(): Promise<void> }
export function validateRules(rules: unknown): Rule[];
export function openBus(options: { directory: string; rules: Rule[]; evaluator: (state: unknown, options: { signal: AbortSignal }) => Promise<Record<string, number>>; evaluatorVersion: string; now?: () => number; evaluationTimeoutMs?: number; sinkTimeoutMs?: number; maxObservations?: number }): Promise<Bus>;
export function webhookSink(url: string, options?: { token?: string; timeoutMs?: number; fetchImpl?: typeof fetch }): Sink;
