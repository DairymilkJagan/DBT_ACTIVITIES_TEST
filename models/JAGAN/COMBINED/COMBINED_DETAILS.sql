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

orders_with_details AS (
    SELECT
        O.*,
        C.*,
        P.*,
        (O.ORDER_SELLING_PRICE - O.ORDER_COST_PRICE) AS ORDER_PROFIT
    FROM
        {{ ref('ORDERS') }} AS O
    LEFT JOIN
        {{ ref('CUSTOMER') }} AS C
    ON 
        C.CUSTOMERID = O.CUSTOMER_ID
    LEFT JOIN
        {{ ref('PRODUCT') }} AS P
    ON
        P.PRODUCTID = O.PRODUCT_ID
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
    OWD.*,
    FM.student_id,
    FM.student_name AS student_name,
    FM.student_age,
    FM.grade,
    FM.teacher_id,
    FM.teacher_name,
    FM.subject,
    FM.experience,
    FM.child_id,
    FM.child_name,
    FM.child_age,
    FM.parent_name
FROM
    orders_with_details AS OWD
LEFT JOIN
    final_merge AS FM
ON
    OWD.ORDER_ID = FM.student_id  -- Adjust join condition as per actual relationship, replace `ORDER_ID` or `student_id` appropriately
