package fr.ifremer.sensornanny.getdata.serverrestful.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Provider
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class JSONMessageBodyHandler implements MessageBodyWriter<Object>, MessageBodyReader<Object> {

    private static final String UTF_8 = "UTF-8";

    @Override
    public boolean isReadable(Class<?> type, Type genericType, java.lang.annotation.Annotation[] annotations,
            MediaType mediaType) {
        return false; // TODO: true si on veut consommer du JSON
    }

    @Override
    public Object readFrom(Class<Object> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException,
                    WebApplicationException {
        BufferedReader reader = null;

        try {

            reader = new BufferedReader(new InputStreamReader(entityStream, UTF_8));
            StringBuilder out = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line);
            }

            try {
                JsonParser parser = new JsonParser();
                // TODO: est-ce que ca fonctionne si on met readable à true et que l'on envoi du JSON ?
                return parser.parse(out.toString());
            } catch (Exception e) {
                throw new WebApplicationException("Error while parsing JSON", e);
            }

        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return true;
    }

    @Override
    public long getSize(Object object, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(Object object, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException,
                    WebApplicationException {
        OutputStreamWriter writer = new OutputStreamWriter(entityStream, UTF_8);
        try {
            if (JsonObject.class.isAssignableFrom(type)) {
                try {
                    writer.append(((JsonObject) object).toString());
                } catch (Exception e) {
                    throw new WebApplicationException("Error while rendering JSON object", e);
                }
            } else if (JsonArray.class.isAssignableFrom(type)) {
                try {
                    writer.append(((JsonArray) object).toString());
                } catch (Exception e) {
                    throw new WebApplicationException("Error while rendering JSON array", e);
                }
            }

        } finally {
            writer.flush();
            writer.close();
        }
    }
}
