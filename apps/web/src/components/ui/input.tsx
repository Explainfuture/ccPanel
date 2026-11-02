import type { ComponentProps } from "react";
import { cn } from "../../lib/utils";
export function Input({ className, ...props }: ComponentProps<"input">) {
  return (
    <input
      data-slot="input"
      className={cn(
        "flex h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-cyan-700 disabled:opacity-50",
        className,
      )}
      {...props}
    />
  );
}
