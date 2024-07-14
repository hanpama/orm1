from dataclasses import dataclass
from datetime import datetime
from orm1 import mapped


@mapped(primary=["semester_id", "subject_id"])
@dataclass(eq=False)
class Course:
    semester_id: str
    subject_id: str
    created_at: datetime
    modules: list["CourseModule"]
    attachments: list["CourseAttachment"]


@mapped()
@dataclass(eq=False)
class CourseAttachment:
    id: str
    media_uri: str
    created_at: datetime


@mapped()
@dataclass(eq=False)
class CourseModule:
    id: str
    title: str
    created_at: datetime
    materials: list["CourseModuleMaterial"]


@mapped()
@dataclass(eq=False)
class CourseModuleMaterial:
    id: str
    media_uri: str
    created_at: datetime
