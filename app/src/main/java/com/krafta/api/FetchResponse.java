package com.krafta.api;

import com.krafta.storage.Record;

import java.util.List;

public record FetchResponse(List<Record> records, long latestOffset) {
}
