Download Link: https://assignmentchef.com/product/solved-b561-assignment-7-key-value-stores-mapreduce-and-spark-2
<br>
<h1></h1>

<ol>

 <li>Consider the document “MapReduce and the New Software Stack” available in the module on MapReduce.<a href="#_ftn1" name="_ftnref1"><sup>[1]</sup></a> In that document, you can, in Sections 2.3.3-2.3.7, find descriptions of algorithms to implement relational algebra operations in MapReduce. (In particular, look at the mapper and reducer functions for various RA operators.)</li>

</ol>

In the following problems, you are asked to write MapReduce programs that implement some RA expressions in PostgreSQL. In addition, you need to add the code which permits the PostgreSQL simulations for these MapReduce programs. A crucial aspect of solving these problem is to develop an appropriate data representation for the input to these problems. Recall that, in MapReduce, the input is a <strong>single </strong>binary relation of (key,value) pairs. Take for example Problem 1c. To solve this problem, you need to find a representation that puts both the data from <em>R </em>and from <em>S </em>in the same binary key-value relation. And, for problem 1d, you need to have a representation that put all the data from <em>R</em>, <em>S</em>, and <em>T </em>in the same binary key-value relation.

<ul>

 <li>Write, in PostgreSQL, a basic MapReduce program, i.e., a mapperfunction and a reducer function, as well as a 3-phases simulation that implements the projection <em>π<sub>A,B</sub></em>(<em>R</em>) where <em>R</em>(<em>A,B,C</em>) is a relation. You can assume that the domains of <em>A</em>, <em>B</em>, <em>C </em>are integer. (Recall that in a projection, duplicates are eliminated.)</li>

 <li>Write, in PostgreSQL, a basic MapReduce program, i.e., a mapperfunction and a reducer function, as well as a 3-phases simulation that implements the set difference of two unary relations <em>R</em>(<em>A</em>) and <em>S</em>(<em>A</em>), i.e., the relation <em>R </em>− <em>S</em>. You can assume that the domain of <em>A </em>is integer.</li>

 <li>Write, in PostgreSQL, a basic MapReduce program, i.e., a mapperfunction and a reducer function, as well as a 3-phases simulation that implements the semijoin <em>R</em>n<em>S </em>of two relations <em>R</em>(<em>A,B</em>) and <em>S</em>(<em>B,C</em>). You can assume that the domains of <em>A</em>, <em>B</em>, and <em>C </em>are integer.</li>

 <li>Let <em>R</em>(<em>A</em>), <em>S</em>(<em>A</em>), and <em>T</em>(<em>A</em>) be unary relations that store integers. Write, in PostgreSQL, a MapReduce program that implements the RA expression <em>R</em>−(<em>S </em>∪<em>T</em>). Also provide a simulation that evaluates this program.</li>

</ul>

<ol start="2">

 <li>Write, in PostgreSQL, a basic MapReduce program, i.e., a mapper function and a reducer function, as well as a 3-phases simulation that implements the SQL query</li>

</ol>

SELECT r.A, array_agg(r.B), cardinality(array_agg(r.B))

FROM       R r

GROUP BY (r.A)

HAVING COUNT(r.B) &gt;= 2;

Here <em>R </em>is a relation with schema (<em>A,B</em>). You can assume that the domains of <em>A </em>and <em>B </em>are integers.

<ol start="3">

 <li>Let <em>R</em>(<em>K,V </em>) and <em>S</em>(<em>K,W</em>) be two binary key-value pair relations. Consider the cogroup transformation cogroup(S) introduced in the lecture on Spark. You can assume that the domains of <em>K</em>, <em>V </em>, and <em>W </em>are integers.</li>

</ol>

<ul>

 <li>Define a PostgreSQL view that represent the co-group transformationcogroup(S). Show that this view works.</li>

 <li>Write a PostgreSQL query that use this cogroup view to compute <em>R</em>n<em>S</em>.</li>

 <li>Write a PostgreSQL query that uses this cogroup view to implement the SQL query</li>

</ul>

SELECT distinct r.K as rK, s.K as sK

FROM          R r, S s

WHERE ARRAY(SELECT r1.V

FROM       R r1

WHERE r1.K = r.K) &lt;@ ARRAY(SELECT s1.W

FROM       S s1

WHERE s1.K = s.K);

<ol start="4">

 <li>Let <em>A </em>and <em>B </em>be two unary relations of integers. Consider the cogroup transformation introduced in the lecture on Spark. Using an approach analogous to the one in Problem 3 solve the following problems:</li>

</ol>

<ul>

 <li>Write a PostgreSQL query that uses the cogroup transformation tocompute <em>A </em>∩ <em>B</em>.</li>

 <li>Write a PostgreSQL query that uses the cogroup operator to computethe symmetric difference of <em>A </em>and <em>B</em>, i.e., the expression</li>

</ul>

(<em>A </em>− <em>B</em>) ∪ (<em>B </em>− <em>A</em>)<em>.</em>

<h1>2           Nested Relations and Semi-structured databases</h1>

<ol start="5">

 <li>Consider the lecture on Nested and Semi-structured Data models. In that lecture, we considered the studentGrades nested relation and we constructed it using a PostgreSQL query starting from the Enroll relation.</li>

</ol>

<ul>

 <li>Write a PostgreSQL query that creates the nested relation courseGrades.</li>

</ul>

The type of this relation is

(cno<em>,</em>gradeInfo{(grade<em>,</em>students{(sid)})})

This relation stores for each course, the grade information of the students enrolled in this course. In particular, for each course and for each grade, this relation stores in a set the students who obtained that grade in that course.

<ul>

 <li>Starting from this nested relation courseGrades, write a PostgreSQL that creates the nested relation studentGrades which is as described in the lecture.</li>

 <li>In the lecture, we defined the jstudentGrades semi-structured relation. Write a PostgreSQL query that creates a jcourseGrades semistructured relation which stores JSON objects whose structure conforms with the structure of tuples as described for the courseGrades nested relation in question 5a.</li>

 <li>Repeat question 5b but now for the semi-structured relations jcourseGrades and the jstudentGrades. In other words, starting from this semistructured relation courseGrades, write a PostgreSQL that creates the semi-structured relation studentGrades.</li>

 <li>In the lecture on Nested and Semi-structured data models, we considered the query “For each student who major in ‘CS’, list his or her sid and sname, along with the courses she is enrolled in. Furthermore, these courses should be grouped by the department in which they are enrolled.” We formulated this query in the context of nested relations.</li>

</ul>

Write a PostgreSQL query to solve this query, but this time for semistructured relations that store only JSON objects.

<h1>3           Graph Databases</h1>

<ol start="6">

 <li>Consider the database schema Student, Course, Buys, Major, and Citesthat we have been using throughout the assignments.

  <ul>

   <li>Specify an Entity-Relationship Diagram that models this databaseschema.</li>

   <li>Specify the node and relationship types of a Property Graph forthis database schema. In addition, specify the properties, if any, associated with each such type.</li>

  </ul></li>

 <li>Using the Property Graph model in Problem 6b, formulate the followingqueries in the Cypher query language:

  <ul>

   <li>Find the types of the relationships associated with Student nodes.</li>

   <li>Find each student (node) whose name is ‘John’ and who bought abook whose price is at least $50.</li>

   <li>Find each student (node) who bought a book that cites a book whoseprice is at least $50.</li>

   <li>Find each book (node) that is cited directly or indirectly (i.e., recursively) by a book that cost more that $50.</li>

  </ul></li>

</ol>

Find for each book node, that node along with the number of studentswho major in both CS and in Math and who bought that book

<a href="#_ftnref1" name="_ftn1">[1]</a> This is Chapter 2 in <em>Mining of Massive Datasets </em>by Jure Leskovec, Anand Rajaraman, and Jeffrey D. Ullman.