from dataclasses import dataclass
from datetime import datetime
from orm1 import auto


@auto.mapped(primary=["semester_id", "subject_id"])
@dataclass(eq=False)
class Course:
    semester_id: str
    subject_id: str
    created_at: datetime
    modules: list["CourseModule"]
    attachments: list["CourseAttachment"]


@auto.mapped()
@dataclass(eq=False)
class CourseAttachment:
    id: str
    media_uri: str
    created_at: datetime


@auto.mapped()
@dataclass(eq=False)
class CourseModule:
    id: str
    title: str
    created_at: datetime
    materials: list["CourseModuleMaterial"]


@auto.mapped()
@dataclass(eq=False)
class CourseModuleMaterial:
    id: str
    media_uri: str
    created_at: datetime
