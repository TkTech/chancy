import {useEffect, useState} from 'react';

function timeSince(datetime: string) {
  const time = new Date(datetime);
  const now = new Date();
  const diff = now.getTime() - time.getTime();
  const hours = Math.floor(diff / 1000 / 60 / 60);
  const minutes = Math.floor(diff / 1000 / 60 % 60);
  const seconds = Math.floor(diff / 1000 % 60);

  const hoursStr = hours < 10 ? `0${hours}` : `${hours}`;
  const minutesStr = minutes < 10 ? `0${minutes}` : `${minutes}`;
  const secondsStr = seconds < 10 ? `0${seconds}` : `${seconds}`;

  return <span>{`${hoursStr}:${minutesStr}:${secondsStr}`}</span>;
}

export const TimeSinceBlock = ({datetime}: { datetime: string }) => {
  const [time, setTime] = useState(timeSince(datetime));

  useEffect(() => {
    const intervalId = setInterval(() => {
      setTime(timeSince(datetime));
    }, 1000);

    return () => clearInterval(intervalId);
  }, [datetime]);

  return <span>{time}</span>;
}

export const TimeBlock = ({datetime}: { datetime: string }) => {
  const time = new Date(datetime);
  const hours = time.getHours();
  const minutes = time.getMinutes();
  const seconds = time.getSeconds();

  const hoursStr = hours < 10 ? `0${hours}` : `${hours}`;
  const minutesStr = minutes < 10 ? `0${minutes}` : `${minutes}`;
  const secondsStr = seconds < 10 ? `0${seconds}` : `${seconds}`;

  return <span>{`${hoursStr}:${minutesStr}:${secondsStr}`}</span>;
}

export const PrettyDateAndTime = ({datetime}: { datetime: string }) => {
  const time = new Date(datetime);
  const hours = time.getHours();
  const minutes = time.getMinutes();
  const seconds = time.getSeconds();

  const hoursStr = hours < 10 ? `0${hours}` : `${hours}`;
  const minutesStr = minutes < 10 ? `0${minutes}` : `${minutes}`;
  const secondsStr = seconds < 10 ? `0${seconds}` : `${seconds}`;

  const date = time.toDateString();

  return <span>{`${date} ${hoursStr}:${minutesStr}:${secondsStr}`}</span>;
}