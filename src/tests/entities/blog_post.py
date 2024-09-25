import uuid
from dataclasses import dataclass
from datetime import datetime
from orm1 import auto


@auto.mapped()
@dataclass(eq=False)
class BlogPost:
    id: uuid.UUID
    title: str
    rating: int | None
    published_at: datetime | None
