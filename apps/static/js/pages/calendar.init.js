/**
* Theme: Dastone - Bootstrap 5 Responsive Admin Dashboard
* Author: Mannatthemes
* Component: Full-Calendar
*/


document.addEventListener('DOMContentLoaded', function() {
  var calendarEl = document.getElementById('calendar');

  var calendar = new FullCalendar.Calendar(calendarEl, {
    headerToolbar: {
      left: 'prev,next today',
      center: 'title',
      right: 'dayGridMonth,timeGridWeek,timeGridDay'
    },
    defaultDate: '2025-01-12',
    timeZone: 'UTC',
    initialView: 'dayGridMonth',
    editable: true,
    selectable: true,
    events: [
      {
        title: 'Business Lunch',
        start: '2025-01-03T13:00:00',
        constraint: 'businessHours',
      },
      {
        title: 'Meeting',
        start: '2025-01-13T11:00:00',
        constraint: 'availableForMeeting', // defined below
      },
      {
        title: 'Conference',
        start: '2025-01-27',
        end: '2025-01-29',
      },

      {
        title: 'Conference',
        start: '2025-03-27',
        end: '2025-02-29',
      },
      
      // areas where "Meeting" must be dropped
      {
        groupId: 'availableForMeeting',
        start: '2025-01-11T10:00:00',
        end: '2025-01-11T16:00:00',
        title: 'Repeating Event',
      },
      {
        groupId: 'availableForMeeting',
        start: '2025-01-15T10:00:00',
        end: '2025-01-15T16:00:00',
        title: 'holiday',
        className: 'bg-soft-danger text-danger',
      },

      {
        groupId: 'availableForMeeting',
        start: '2025-02-15T10:00:00',
        end: '2025-02-15T16:00:00',
        title: 'holiday',
        className: 'bg-soft-danger text-danger',
      },

      // red areas where no events can be dropped
      
      {
        start: '2025-01-06',
        end: '2025-01-08',
        overlap: false,
        title: 'New Event',
      }
    ],
  });

  calendar.render();
});

