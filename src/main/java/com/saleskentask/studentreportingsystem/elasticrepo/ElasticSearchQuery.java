package com.saleskentask.studentreportingsystem.elasticrepo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.saleskentask.studentreportingsystem.model.FirstSem;
import com.saleskentask.studentreportingsystem.model.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.*;

@Repository
public class ElasticSearchQuery {

    @Autowired
    private ElasticsearchClient elasticsearchClient;

    private final String indexName = "students";


    public String createOrUpdateDocument(Student student) throws IOException {

        IndexResponse response = elasticsearchClient.index(i -> i
                .index(indexName)
                .id(student.getId())
                .document(student)
        );
        if(response.result().name().equals("Created")){
            return new StringBuilder("Document has been successfully created.").toString();
        }else if(response.result().name().equals("Updated")){
            return new StringBuilder("Document has been successfully updated.").toString();
        }
        return new StringBuilder("Error while performing the operation.").toString();
    }

    public Student getDocumentById(String studentId) throws IOException{
        Student student = null;
        GetResponse<Student> response = elasticsearchClient.get(g -> g
                        .index(indexName)
                        .id(studentId),
                Student.class
        );

        if (response.found()) {
            student = response.source();
            System.out.println("student name " + student.getName());
        } else {
            System.out.println ("student not found");
        }

        return student;
    }

    public String deleteDocumentById(String studentId) throws IOException {

        DeleteRequest request = DeleteRequest.of(d -> d.index(indexName).id(studentId));

        DeleteResponse deleteResponse = elasticsearchClient.delete(request);
        if (Objects.nonNull(deleteResponse.result()) && !deleteResponse.result().name().equals("NotFound")) {
            return new StringBuilder("student with id " + deleteResponse.id() + " has been deleted.").toString();
        }
        System.out.println("student not found");
        return new StringBuilder("student with id " + deleteResponse.id()+" does not exist.").toString();

    }

    public  List<Student> searchAllDocuments() throws IOException {

        SearchRequest searchRequest =  SearchRequest.of(s -> s.index(indexName));
        SearchResponse searchResponse =  elasticsearchClient.search(searchRequest, Student.class);
        List<Hit> hits = searchResponse.hits().hits();
        List<Student> students = new ArrayList<>();
        for(Hit object : hits){

            System.out.print(((Student) object.source()));
            students.add((Student) object.source());

        }
        return students;
    }

    public Double getAverageStudent(String name, String subject) throws IOException {
        SearchRequest searchRequest = SearchRequest.of(s -> s
                .index(indexName)
                .query(q -> q
                        .match(t -> t
                                .field("name")
                                .query(name))));

        SearchResponse<Student> response = elasticsearchClient.search(searchRequest, Student.class);
        List<Hit<Student>> hits = response.hits().hits();
        Student currStudent = (Student) hits.get(0).source();


        int sum = 0;
        switch (subject) {
            case "english": sum += currStudent.getFirstSem().getEnglish();
                            sum += currStudent.getSecondSem().getEnglish();
                            break;
            case "maths": sum += currStudent.getFirstSem().getMaths();
                            sum += currStudent.getSecondSem().getMaths();
                            break;
            case "science":sum += currStudent.getFirstSem().getScience();
                        sum += currStudent.getSecondSem().getScience();
                        break;
        }

        double res = sum/(double)2;
        return res;
    }
    public List<Student> top2Consistent() throws IOException {
        SearchRequest searchRequest =  SearchRequest.of(s -> s.index(indexName));
        SearchResponse searchResponse =  elasticsearchClient.search(searchRequest, Student.class);
        List<Hit> hits = searchResponse.hits().hits();
        List<Student> students = new ArrayList<>();
        for(Hit object : hits){
            System.out.print(((Student) object.source()));
            students.add((Student) object.source());
        }

        PriorityQueue<Student> pq = new PriorityQueue<Student>(new StudentComparator());
        for(Student student : students) {
            pq.add(student);
        }

        List<Student> res = new ArrayList<>();
        if(pq.size()>0) {
            res.add(pq.peek());
            pq.poll();
        }
        if(pq.size()>0) {
            res.add(pq.peek());
            pq.poll();
        }

        return res;

    }

    public Double getAverageWholeClass(int sem) throws IOException {
        SearchRequest searchRequest =  SearchRequest.of(s -> s.index(indexName));
        SearchResponse searchResponse =  elasticsearchClient.search(searchRequest, Student.class);
        List<Hit> hits = searchResponse.hits().hits();
        List<Student> students = new ArrayList<>();
        for(Hit object : hits){
            System.out.print(((Student) object.source()));
            students.add((Student) object.source());
        }

        double sum = 0;
        double count = 0;
        if(sem == 1) {
            for (Student x : students) {
                sum += x.getFirstSem().getEnglish();
                sum += x.getFirstSem().getMaths();
                sum += x.getFirstSem().getScience();
                count+=3;
            }
        }
        else {
            for (Student x : students) {
                sum += x.getSecondSem().getEnglish();
                sum += x.getSecondSem().getMaths();
                sum += x.getSecondSem().getScience();
                count+=3;
            }
        }

        Double avg = sum/count;
        return avg;

    }

    public Double getAverageSubject(String subject) throws IOException {
        SearchRequest searchRequest =  SearchRequest.of(s -> s.index(indexName));
        SearchResponse searchResponse =  elasticsearchClient.search(searchRequest, Student.class);
        List<Hit> hits = searchResponse.hits().hits();
        List<Student> students = new ArrayList<>();
        for(Hit object : hits){
            System.out.print(((Student) object.source()));
            students.add((Student) object.source());
        }

        double sum = 0;
        double count = 0;
        switch (subject) {
            case "english": for(Student student : students) {
                                sum += student.getFirstSem().getEnglish();
                                sum += student.getSecondSem().getEnglish();
                                count += 2;
                            }
                            break;

            case "maths": for(Student student : students) {
                                sum += student.getFirstSem().getMaths();
                                sum += student.getSecondSem().getMaths();
                                count += 2;
                            }
                            break;
            case "science": for(Student student : students) {
                                sum += student.getFirstSem().getScience();
                                sum += student.getSecondSem().getScience();
                                count += 2;
                            }
                            break;
        }

        double avg = sum/count;
        return avg;
    }

    class StudentComparator implements Comparator<Student> {

        // Overriding compare()method of Comparator
        // for descending order of cgpa
        public int compare(Student s1, Student s2) {
            double sum1 = s1.getFirstSem().getEnglish()
                    +s1.getFirstSem().getMaths()
                    +s1.getFirstSem().getScience()
                    +s1.getSecondSem().getEnglish()
                    +s1.getSecondSem().getMaths()
                    +s1.getSecondSem().getScience();

            double sum2 = s2.getFirstSem().getEnglish()
                    +s2.getFirstSem().getMaths()
                    +s2.getFirstSem().getScience()
                    +s2.getSecondSem().getEnglish()
                    +s2.getSecondSem().getMaths()
                    +s2.getSecondSem().getScience();
            return sum1<sum2?1:sum1==sum2?0:-1;
        }
    }

}
