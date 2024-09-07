import { Prisma, PrismaClientExtends } from "@prisma/client/extension";
import { DefaultArgs } from "@prisma/client/runtime/library";

import { Readable } from "node:stream";
import type { ReadableStream } from "node:stream/web";

export default Prisma.defineExtension(
  (client: PrismaClientExtends<DefaultArgs>) => {
    return client.$extends({
      model: {
        $allModels: {
          cursorStream<
            T,
            A extends Prisma.Args<T, "findMany"> | undefined,
            R extends Prisma.Result<T, A, "findMany">[number]
          >(
            this: T,
            findManyArgs: A,
            { batchSize, prefill } = {} as {
              batchSize?: number;
              prefill?: number;
            }
          ): ReadableStream<R> {
            findManyArgs = findManyArgs ?? ({} as A);
            const context = Prisma.getExtensionContext(this);

            const take = batchSize || 100;
            const highWaterMark = prefill || take * 2;
            const cursorField =
              Object.keys(findManyArgs.cursor || {})[0] || "id";

            if (findManyArgs.select && !findManyArgs.select[cursorField]) {
              throw new Error(`Must select cursor field "${cursorField}"`);
            }

            let cursorValue: number;
            const readableStream = new Readable({
              objectMode: true,
              highWaterMark,
              async read() {
                try {
                  const results = await (context as any).findMany({
                    ...findManyArgs,
                    take,
                    skip: cursorValue ? 1 : 0,
                    cursor: cursorValue
                      ? {
                          [cursorField]: cursorValue,
                        }
                      : undefined,
                  });
                  for (const result of results) {
                    this.push(result);
                  }
                  if (results.length < take) {
                    this.push(null);
                    return;
                  }
                  cursorValue = (<any>results[results.length - 1])[cursorField];
                } catch (err: any) {
                  this.destroy(err);
                }
              },
            });

            return Readable.toWeb(readableStream);
          },
        },
      },
    });
  }
);
