package com.saleskentask.studentreportingsystem.elasticrepo;

import com.saleskentask.studentreportingsystem.model.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
public class ElasticSearchController {

    @Autowired
    private ElasticSearchQuery elasticSearchQuery;

    @PostMapping("/createOrUpdateDocument")
    public ResponseEntity<Object> createOrUpdateDocument(@RequestBody Student student) throws IOException {
        String response = elasticSearchQuery.createOrUpdateDocument(student);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/getDocument")
    public ResponseEntity<Object> getDocumentById(@RequestParam String studentId) throws IOException {
        Student student =  elasticSearchQuery.getDocumentById(studentId);
        return new ResponseEntity<>(student, HttpStatus.OK);
    }

    @DeleteMapping("/deleteDocument")
    public ResponseEntity<Object> deleteDocumentById(@RequestParam String studentId) throws IOException {
        String response =  elasticSearchQuery.deleteDocumentById(studentId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/searchDocument")
    public ResponseEntity<Object> searchAllDocument() throws IOException {
        List<Student> students = elasticSearchQuery.searchAllDocuments();
        return new ResponseEntity<>(students, HttpStatus.OK);
    }

    @GetMapping("/getAverageStudent")
    public ResponseEntity<Double> getAverage(@RequestParam String name, @RequestParam String subject) throws IOException {
        Double avg = elasticSearchQuery.getAverageStudent(name,subject);
        return new ResponseEntity<>(avg, HttpStatus.OK);
    }

    @GetMapping("/getAverageWholeClass")
    public ResponseEntity<Double> getAverageWholeClass(@RequestParam Integer sem) throws IOException {
        Double avg = elasticSearchQuery.getAverageWholeClass(sem);
        return new ResponseEntity<>(avg, HttpStatus.OK);
    }

    @GetMapping("/top2Consistent")
    public ResponseEntity<Object> getAverage() throws IOException {
        List<Student> top2 = elasticSearchQuery.top2Consistent();
        return new ResponseEntity<>(top2, HttpStatus.OK);
    }

    @GetMapping("/getAverageSubject")
    public ResponseEntity<Double> getAverageSubject(@RequestParam String subject) throws IOException {
        Double avg = elasticSearchQuery.getAverageSubject(subject);
        return new ResponseEntity<>(avg,HttpStatus.OK);
    }
}