package com.intropro.prairie.unit.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by presidentio on 10/14/15.
 */
public class ConstSource extends AbstractSource implements Configurable, PollableSource {

    private Queue<String> queue;


    @Override
    public void configure(Context context) {
        String items = context.getString("items");
        queue = new LinkedList<>();
        queue.addAll(Arrays.asList(items.split(",")));
    }

    @Override
    public void start() {
    }


    @Override
    public void stop() {
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        String msg = queue.poll();
        if (msg == null) {
            return Status.BACKOFF;
        }
        Event event = EventBuilder.withBody(msg.getBytes());
        getChannelProcessor().processEvent(event);

        return status;

    }
}
