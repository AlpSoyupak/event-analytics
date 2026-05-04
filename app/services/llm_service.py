from groq import AsyncGroq

from app.config import get_settings

settings = get_settings()


class LLMService:
    def __init__(self):
        self._client: AsyncGroq | None = None

    def _get_client(self) -> AsyncGroq:
        if self._client is None:
            self._client = AsyncGroq(api_key=settings.groq_api_key)
        return self._client

    async def chat_with_tools(
        self,
        messages: list[dict],
        tools: list[dict],
        max_tokens: int = 1024,
    ) 
    :
        response = await self._get_client().chat.completions.create(
            model=settings.groq_model,
            messages=messages,
            tools=tools,
            tool_choice="auto",
            max_tokens=max_tokens,
        )
        return response.choices[0].message

    async def chat(
        self,
        messages: list[dict],
        max_tokens: int = 2048,
    ) -> str:
        response = await self._get_client().chat.completions.create(
            model=settings.groq_model,
            messages=messages,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content or ""


llm_service = LLMService()
