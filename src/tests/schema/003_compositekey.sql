CREATE TABLE course (
    semester_id VARCHAR(20),
    subject_id VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY(semester_id, subject_id)
);

CREATE TABLE course_attachment (
    id VARCHAR(20) PRIMARY KEY,
    course_semester_id VARCHAR(20) NOT NULL,
    course_subject_id VARCHAR(20) NOT NULL,
    media_uri TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (course_semester_id, course_subject_id) REFERENCES course(semester_id, subject_id)
);

CREATE TABLE course_module (
    id VARCHAR(20) PRIMARY KEY,
    course_semester_id VARCHAR(20) NOT NULL,
    course_subject_id VARCHAR(20) NOT NULL,

    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (course_semester_id, course_subject_id) REFERENCES course(semester_id, subject_id)
);

CREATE TABLE course_module_material (
    id VARCHAR(20) PRIMARY KEY,
    course_module_id VARCHAR(20) NOT NULL REFERENCES course_module(id),
    media_uri TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
