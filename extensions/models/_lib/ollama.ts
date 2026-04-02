export async function ollamaGenerate(
  ollamaUrl: string,
  model: string,
  prompt: string,
  input: string,
): Promise<string> {
  const resp = await fetch(`${ollamaUrl}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model,
      messages: [{ role: "user", content: `${prompt}\n\n${input}` }],
      stream: false,
      options: { num_predict: 1024 },
      think: false,
    }),
  });

  if (!resp.ok) {
    throw new Error(`Ollama error (${resp.status}): ${await resp.text()}`);
  }

  // deno-lint-ignore no-explicit-any
  const json: any = await resp.json();
  let raw = (json.message?.content ?? "").trim();
  if (raw.startsWith("```")) {
    raw = raw.replace(/^```(?:json)?\n?/, "").replace(/\n?```$/, "");
  }
  return raw;
}
