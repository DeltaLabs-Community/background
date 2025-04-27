import { JobQueue } from "../../src";

declare module "hono" {
    interface ContextVariableMap {
        queues?: JobQueue[];
    }
}
