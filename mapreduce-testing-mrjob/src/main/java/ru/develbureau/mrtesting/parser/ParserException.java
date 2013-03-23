package ru.develbureau.mrtesting.parser;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 13:49
 */
public class ParserException extends Exception{

    public ParserException(String message) {
        super(message);
    }

    public ParserException(String message, Throwable cause) {
        super(message, cause);
    }
}
