-- File: merge.sql

WITH students_with_teachers AS (
    SELECT
        S.student_id,
        S.student_name,
        S.age AS student_age,
        S.grade,
        T.teacher_id,
        T.teacher_name,
        T.subject,
        T.experience
    FROM
        {{ ref('STUDENTS') }} AS S
    LEFT JOIN
        {{ ref('TEACHERS') }} AS T
    ON
        S.grade = T.subject  -- Assuming students are taught by teachers of the same subject
),

final_merge AS (
    SELECT
        SW.student_id,
        SW.student_name,
        SW.student_age,
        SW.grade,
        SW.teacher_id,
        SW.teacher_name,
        SW.subject,
        SW.experience,
        C.child_id,
        C.child_name,
        C.age AS child_age,
        C.parent_name
    FROM
        students_with_teachers AS SW
    LEFT JOIN
        {{ ref('CHILDREN') }} AS C
    ON
        C.parent_name = SW.student_name  -- Assuming a child is related to a student by the parent name
)

SELECT
    *
FROM
    final_merge
