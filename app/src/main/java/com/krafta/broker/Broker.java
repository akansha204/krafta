package com.krafta.broker;

import com.krafta.exceptions.TopicAlreadyExistsException;
import com.krafta.exceptions.TopicNotFoundException;
import com.krafta.storage.Message;
import com.krafta.storage.Partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Broker {
    public Map<String, Partition> topics = new HashMap<>();
    public void createTopic(String topicName) throws TopicAlreadyExistsException, IOException {
        if (topics.containsKey(topicName)) {
            throw new TopicAlreadyExistsException("Topic already exists");
        }
        topics.put(topicName, new Partition("../data/" + topicName));
    }
    public void send(String topicName, String message) throws TopicNotFoundException, IOException {
        if (!topics.containsKey(topicName)) {
            throw new TopicNotFoundException("Topic not found");
        }
        topics.get(topicName).append(message);
    }
    public List<Message> consume(String topic, long offset, long maxMessages) throws TopicNotFoundException, IOException {
        if(!topics.containsKey(topic)){
            throw new TopicNotFoundException("Topic not found");
        }
        List<Message> messages = new ArrayList<>();
        for(int i=0;i<maxMessages;i++){
            try{
                Message msg = topics.get(topic).read(offset);
                messages.add(msg);
                offset++;
            } catch (Exception e) {
                break;
            }
        }
        return messages;
    }
}
